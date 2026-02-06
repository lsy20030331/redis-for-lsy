package com.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Hello world!
 *
 */
public class RedisServer
{

    // Redis的16个数据库实例
    static RedisDB[] redisDB;
    // 保存所有客户端连接的列表
    static List<RedisClient> clients = new ArrayList<>();
    // 服务器套接字通道，用于监听客户端连接
    static ServerSocketChannel serverSocketChannel;
    // 多路复用选择器，用于管理多个客户端连接的I/O操作
    static Selector selector;
    // 映射SelectionKey到对应RedisClient的哈希表，便于快速查找客户端
    static HashMap<SelectionKey, RedisClient> clientsMap = new HashMap<>();

    // redisDb的结构
    static class RedisDB{
        // 存储键值对
        Dict dict = new Dict();
        // 存储键值对过期时间
        Dict expires = new Dict();

        public Object get(String key) {
            return dict.get(key);
        }

        public void set(String key, Object value) {
            dict.set(key, value);
        }
    }

    // 字典
    static class Dict{
        // 渐进式哈希所以要有两张哈希表
        Hashtable[] ht = new Hashtable[2];

        {
            // 初始化哈希表
            for (int i = 0; i < 2; i++) {
                ht[i] = new Hashtable();
            }
        }

        // -1表示没有在进行渐进式哈希
        int rehash = -1;

        public Object get(String key) {
            if (rehash == 1){
                // 正在进行渐进式哈希, 需要判断元素是在h[0]还是h[1]
                Object value = ht[0].get(key);
                if (value != null){
                    return value;
                }else{
                    return ht[1].get(key);
                }
            }else {
                return ht[0].get(key);
            }
        }

        public void set(String key, Object value) {
            if (rehash == -1){
                ht[0].put(key, value); // 没有进行渐进式哈希
            }else {
                ht[1].put(key, value); // 正在进行渐进式哈希
            }
        }
    }

    // 客户端
    static class RedisClient{
        // 当前连接的redis数据库
        RedisDB selectDB;

        byte[] queryBuf = new byte[1024];  // 输入缓冲区
        int queryBufLen;  // 输入缓冲区长度

        byte[] outBuf = new byte[1024];  // 输出缓冲区
        int outBufLen;  // 输出缓冲区长度

        // ===== 非redis数据结构 =====
        Object returnValue; // 返回值
        SocketChannel channel; // NIO连接

        boolean read;  // 是否可读
        boolean write;  // 是否可写
        boolean accept;  // 是否接收连接


        // 将ByteBuffer的数据追加到queryBuf中
        public void appendToQueryBuf(ByteBuffer buffer){
            // 切换到读模式(将指针切换到起点从头开始读)
            buffer.flip();
            try{
                // 直接复制全部可读数据

                // 计算出这次一共收到了多少个字节的数据
                int bytesToCopy = buffer.remaining();
                // 此方法用来确保容量足够
                ensureQueryBufCapacity(bytesToCopy);
                buffer.get(queryBuf, 0, bytesToCopy);
                queryBufLen = bytesToCopy;
            }finally {
                // 无论是否成功都必须清空缓冲区
                buffer.clear();
            }
        }

        // 动态扩容
        private void ensureQueryBufCapacity(int bytesToCopy) {
            if (queryBuf.length < queryBufLen + bytesToCopy){
                // 如空间不够那么就扩容到原来的2倍
                int newCapacity = Math.max(queryBuf.length * 2, queryBufLen + bytesToCopy);
                queryBuf = Arrays.copyOf(queryBuf, newCapacity);
            }
        }
    }


    public static void main( String[] args ) throws IOException, InterruptedException {
        initServer();

        // 下次执行公事（例如持久化等操作）的时间
        long nextEventTime = System.currentTimeMillis();
        while(true){
            // 记录当前时间
            long now = System.currentTimeMillis();
            long timeout = 0;
            if (now < nextEventTime){
                /**
                 * 还未到达下次公事的时间
                 * 计算：超时时间 = 下次执行公事时间 - 当前时间
                 */
                timeout = nextEventTime - now;
            }else if (now > nextEventTime){
                /**
                 * 已经到达下次公事时间
                 * 1. 执行公事
                 * 2. 计算：下次公事时间 = 当前时间 + 公事间隔
                 */
                nextEventTime = now + 100;
            }

            // 在容忍时间(timeout)里处理请求事件
            aeProcessEvents(timeout);

            // 当前时间比下一次处理公共事件还要大那么就处理公事
            if (now >= nextEventTime){
                serverCron();
            }
        }
    }



    // 初始化redis服务
    public static void initServer() throws IOException {
        /**
         * 从配置文件读取配置信息
         */
        int dbCount = 16;
        int port = 6379;

        redisDB = new RedisDB[dbCount];
        for (int i = 0; i < dbCount; i++){
            redisDB[i] = new RedisDB();
        }

        // 打开通道
        serverSocketChannel = serverSocketChannel.open();
        // 设置为不阻塞
        serverSocketChannel.configureBlocking(false);
        // 绑定6379端口
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        // 打开选择器
        selector = Selector.open();
        // 注册channel到选择器
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    // 处理请求事件
    private static void aeProcessEvents(long timeout) throws IOException {
        // 此方法会使当前线程进入休眠状态, 直到有I/O事件发生或者超时
        selector.select(timeout);
        // 将所有选择键移到selectedKeys集合中(SelectionKey表示一个channel在Selector上注册它包含了该channel的事件兴趣和状态)
        Set<SelectionKey> selectionKeys = selector.selectedKeys();
        // 使用迭代器遍历selectedKeys集合
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()){
            // 取出一个选择键
            SelectionKey key = iterator.next();
            // 将选择键从selectedKeys集合中移除，以防误重复处理
            iterator.remove();
            // 处理事件
            System.out.println("处理事件:" + key);

            if (key.isAcceptable()){
                // 接收连接
                handleAccept(key);
            }else if (key.isReadable()){
                handleRead(key);
            }else if (key.isWritable()){
                // 发送数据
                handleWrite(key);
            }
        }
    }

    /**
     * 连接事件
     * @param key
     * @throws IOException
     */
    public static void handleAccept(SelectionKey key) throws IOException {
        RedisClient redisClient = new RedisClient();
        // 接受连接
        SocketChannel clientChannel = serverSocketChannel.accept();
        if (clientChannel == null) return;


        System.out.println("接受连接" + clientChannel);
        clientChannel.configureBlocking(false);

        /*==注册可读事件==*/
        clientChannel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(1024));
        // 为客户端绑定socket
        redisClient.channel = clientChannel;
        // 默认选择0号数据库
        redisClient.selectDB = redisDB[0];

        // 将选择器与当前客户端绑定以便操作
        clientsMap.put(clientChannel.keyFor(selector), redisClient);
        clients.add(redisClient);
    }

    /**
     * 处理读事件
     * @param key
     * @throws IOException
     */
    public static void handleRead(SelectionKey key) throws IOException {
        // 因为已经注册到了map中所以可以直接通过key去取出redisClient
        RedisClient redisClient = clientsMap.get(key);
        if (redisClient == null){
            return;
        }
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        try {
            // 读取数据
            int bytesRead = socketChannel.read(buffer);
            if (bytesRead == -1){
                closeClient(socketChannel, key, redisClient);
                return;
            }
        } catch (IOException e) {
            closeClient(socketChannel, key, redisClient);
        }
        System.out.println("读取数据" +  new String(buffer.array()));

        // 将缓冲区数据移动到queryBuf中
        redisClient.appendToQueryBuf(buffer);
        // 处理客户端缓冲区的内容
        processQueryBuf(redisClient);
    }


    /**
     * 处理写事件
     * @param key
     * @throws IOException
     */
    public static void handleWrite(SelectionKey key) throws IOException {
        RedisClient client = clientsMap.get(key);
        if (client == null){
            return;
        }
        Object returnValue = client.returnValue;

        SocketChannel socketChannel = (SocketChannel) key.channel();

        // ============= 新增 RESP 协议封装逻辑 =============
        if (client.returnValue == null) {
            // redis resp null处理 "$"代表是字符串 resp必须以"\r\n"结尾
            client.outBuf = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
        } else if (client.returnValue instanceof String) {
            // 如果是String类型
            String rawValue = (String) client.returnValue;
            // 判断响应类型（示例逻辑，需根据实际命令完善）
            if ("OK".equals(rawValue)) {
                // 如果返回OK那么就按照简单字符串的resp协议来进行封装
                client.outBuf = RespUtil.formatSimpleString(rawValue); // +OK\r\n
            } else {
                // 其他字符串就按大字符串的resp协议进行封装 "$长度\r\n内容\r\n"
                client.outBuf = RespUtil.formatBulkString(rawValue);    // $5\r\n内容\r\n
            }
        } else if (client.returnValue instanceof Integer) {
            // 如果是Integer类型那么就按:数值"\r\n"进行封装
            client.outBuf = RespUtil.formatInteger((Integer) client.returnValue); // :42\r\n
        } else if (client.returnValue instanceof Long) {
            Long retValue = (Long) client.returnValue;
            int i = retValue.intValue();
            client.outBuf = RespUtil.formatInteger(i); // :42\r\n
        }  else if (client.returnValue instanceof Throwable) {
            // 如果是错误描述那么封装为 "-ERR 错误描述\r\n"
            Throwable status = (Throwable) client.returnValue;
            client.outBuf = RespUtil.formatError(status.getMessage()); // -ERR...
        }

        // ================================================

        // 将字节数组包装成 Buffer，因为 NIO 的 Channel 只认 Buffer
        ByteBuffer buffer = ByteBuffer.wrap(client.outBuf);
        try {
            int totalWritten = 0;
            // 如果buffer中还有数据没有写入
            while (buffer.hasRemaining()) {
                int bytesWritten = socketChannel.write(buffer);
                // 如果wirte()返回0，则说明网络缓冲区被瞬间写满了必须立刻break
                if (bytesWritten <= 0) break;
                // 如果正常写入就将写入长度的记录加上
                totalWritten += bytesWritten;
            }

            // 如果buffer中还有数据没有写完
            if (buffer.hasRemaining()) {
                // 分片写入优化
                if (totalWritten < client.outBuf.length) {
                    client.outBuf = Arrays.copyOfRange(
                            client.outBuf, totalWritten, client.outBuf.length
                    );
                }
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                client.write = true;
            } else {
                // 状态清零与事件切换
                //client.outBuf = null;
                client.returnValue = null;
                // 取消写事件
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                //key.interestOps(SelectionKey.OP_READ);
                client.write = false;
            }
        } catch (Exception e) {
            System.out.println("write error: " + e.getMessage());
            closeClient(socketChannel, key, client); // 需实现资源释放
        }
    }

    /**
     * 处理客户端请求的缓冲区
     * TCP 拆包，封包
     * 1: queryBuf不能构成一个完整的resp协议 (指令不完整)
     * 2: queryBuf刚好构成一个完整的resp协议 (刚好一个完整)
     * 3: queryBuf不能构成两个完整的resp，但是可以构成一个完整的resp (一个完整，另一个不完整)
     * 4: queryBuf可以构成多个完整的resp(2个以上) (都是完整的)
     */
    private static void processQueryBuf(RedisClient client) {
        while (client.queryBufLen > 0) {
            RedisRequest redisRequest = new RedisRequest();
            // 读取命令
            int processed = RespUtil.parseCommand(client.queryBuf, 0, client.queryBufLen, redisRequest);

            if (processed > 0) {
                // 处理完整命令
                // 内存平移
                // 切割剩下的数据(虽然读进来了，但还没来得及处理的残余字节)
                byte[] remaining = Arrays.copyOfRange(client.queryBuf, processed, client.queryBufLen);
                // 将剩下的数据复制到缓冲区头部
                System.arraycopy(remaining, 0, client.queryBuf, 0, remaining.length);
                // 更新缓冲区的长度
                client.queryBufLen = remaining.length;
                // 执行命令
                Object result = doCommand(client, redisRequest);
                // 标记回复状态
                client.returnValue = result;
                client.write = true;

                // 注册写事件
                SelectionKey key = client.channel.keyFor(selector);
                // 叠加事件(让一个连接具备多种状态)
                // eg: 为了不丢失之前事件的状态，需要使用位运算叠加
                // 如果客户端在发完第一个指令后紧接着发了第二个指令，服务器就会因为不再关注“读”事件而完全听不到，导致请求丢失或延迟
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            } else if (processed == 0) {
                break; // 数据不完整，等待下次读取
            } else {
                //closeClient(client.channel, client.channel.keyFor(selector), client);
                break;
            }
        }
    }

    public static Object doCommand(RedisClient redisClient, RedisRequest redisRequest) {
        System.out.println("handle command: " + redisRequest.command + " args: " + redisRequest.args);

        if ("get".equalsIgnoreCase(redisRequest.command)){
            // 处理get命令
            return redisClient.selectDB.get(redisRequest.args.get(0));
        }else if("set".equalsIgnoreCase(redisRequest.command)){
            // 处理set命令
            redisClient.selectDB.set(redisRequest.args.get(0), redisRequest.args.get(1));
            return "OK";
        }else if ("select".equalsIgnoreCase(redisRequest.command)){
            // 处理select命令
            int dbIndex = Integer.parseInt(redisRequest.args.get(0));
            if (dbIndex < 0 || dbIndex >= redisDB.length){
                return "ERR invalid DB index";
            }
            redisClient.selectDB = redisDB[dbIndex];
            return "OK";
        }else if ("auth".equalsIgnoreCase(redisRequest.command)){

        }
        return "ERR unknown command '" + redisRequest.command + "'";
    }

    /**
     * 客户端请求信息
     */
    static class RedisRequest{
        String command;
        List<String> args;
    }

    private static void closeClient(SocketChannel socketChannel, SelectionKey key, RedisClient redisClient) throws IOException {
        socketChannel.close();
        clientsMap.remove(key);
        clients.remove(redisClient);
        System.out.println("关闭连接" + socketChannel);
    }

    public static void serverCron() throws InterruptedException {
        /**
         * 此处省略公事（包括但不限）
         * 1. 关闭redis服务器
         * 2. 持久化数据
         * 3. 删除过期数据
         * 4. 渐进式rehash
         * 5. 集群故障转移等
         */
        Thread.sleep(10);
    }
}
