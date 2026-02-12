package com.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;

/**
 * Hello world!
 *
 */
public class RedisServer
{
    /* Redis 内存淘汰策略 */
    static int REDIS_MAXMEMORY_VOLATILE_LRU = 0;
    static int REDIS_MAXMEMORY_VOLATILE_TTL = 1;
    static int REDIS_MAXMEMORY_VOLATILE_RANDOM = 2;

    static int REDIS_MAXMEMORY_ALLKEYS_LRU = 3;
    static int REDIS_MAXMEMORY_ALLKEYS_RANDOM = 4;
    static int REDIS_MAXMEMORY_NO_EVICTION = 5;
    static int REDIS_DEFAULT_MAXMEMORY_POLICY = REDIS_MAXMEMORY_NO_EVICTION;

    static long maxmemory = 3;//模拟最大内存大小
    static int maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;

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
        public Dict<RedisObject> dict = new Dict();
        // 存储键值对过期时间
        public Dict<Long> expires = new Dict();
        // 数据库的索引
        public int id;

        // 表示redisDB所对应的客户端以及客户端监控的key
        public List<WatchedKeyClient> watched_keys = new ArrayList<>();
        public static class WatchedKeyClient{
            RedisClient redisClient;
            String key;
        }
    }

    // 字典
    static class Dict<T>{
        // 渐进式哈希所以要有两张哈希表
        Hashtable<String, T>[] ht = new Hashtable[2];

        {
            // 初始化哈希表
            for (int i = 0; i < 2; i++) {
                ht[i] = new Hashtable();
            }
        }

        // -1表示没有在进行渐进式哈希
        int rehash = -1;
        
        // 通用代码块
        public void set(String key, T value){
            if (rehash == -1){
                // 如果没有进行渐进式哈希
                ht[0].put(key, value);
            }else {
                // 如果正在进行渐进式哈希
                ht[1].put(key, value);
            }
        }

        public void remove(String key){
            if (rehash == -1){
                // 如果没有进行渐进式哈希
                ht[0].remove(key);
            }else {
                // 如果正在进行渐进式哈希
                ht[1].remove(key);
            }
        }

        public long getDictSize(){
            return ht[0].size() + ht[1].size();
        }

        // dict方法
        public RedisObject getRedisObject(String key){
            if (rehash == 1){
                RedisObject redisObject = (RedisObject) ht[0].get(key);
                if (redisObject != null){
                    return redisObject;
                }else {
                    return (RedisObject) ht[1].get(key);
                }
            }else {
                return (RedisObject) ht[0].get(key);
            }
        }

        // 获取redis的IDEL时间 (数据多久没有被动过了)
        public Long getIDLE(String key){
            RedisObject redisObject = this.getRedisObject(key);
            return redisObject == null ? null : System.currentTimeMillis() - redisObject.lru;
        }

        // expire方法
        public Long getTTL(String key){
            if (rehash == 1){
                Long expireTime = (Long) ht[0].get(key);
                if (expireTime != null){
                    return expireTime;
                }else {
                    return (Long) ht[1].get(key);
                }
            }else {
                return (Long) ht[0].get(key);
            }
        }
    }

    static class RedisObject {
        int type;//类型
        int encoding;//编码

        long lru;//最近一次被访问的时间戳

        int refcount;//引用计数
        Object value;

        public RedisObject(Object value) {
            this.lru = System.currentTimeMillis();
            this.value = value;
        }

        @Override
        public String toString() {
            return "RedisObject{" +
                    "type=" + type +
                    ", encoding=" + encoding +
                    ", lru=" + lru +
                    ", refcount=" + refcount +
                    ", value=" + value +
                    '}';
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
        // 双向记录表示订阅哪些channel
        List<String> subscribedChannels = new ArrayList<>();

        boolean read;  // 是否可读
        boolean write;  // 是否可写
        boolean accept;  // 是否接收连接

        /**
         * 客户端状态
         * 0: slave
         * 1: master
         * 2: slave monitor
         * 3: multi 事务中 $$$
         * 4: blocked
         * 5: watched key modified $$$
         */
        int flags = 0;
        Multi.MultiState multiState;

        // 当前这个客户端监控的键
        List<WatchedKey> watched_keys = new ArrayList<>();

        public static class WatchedKey{
            String key; // 代表监控的哪个key
            int db; // 表示第几号数据库
        }

        BlockingState bpop;
        public static class BlockingState {
            long timeout; // 超时时间
            Set<String> keys = new HashSet<>(); // 阻塞的key
        }

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

    // 订阅频道key表示channel value表示channel对应的redisClient
    static Map<String, List<RedisClient>> pubsub_Channels = new HashMap<>();

    // 数组对象
    static class ArrayObject {
        Object[] elements;

        // 表示任意数量的参数
        ArrayObject(Object... elements) {
            this.elements = elements;
        }
    }

    // 时间事件
    public static long eventTime = System.currentTimeMillis();

    // 引入redis hz动态管理频率
    static int hz = 10;


    private static void beforeSleep() {
        // 过期键的主动删除
        activeExpireCycle(true);
    }
    public static void main( String[] args ) throws IOException, InterruptedException {
        initServer();

        // 下次执行公事（例如持久化等操作）的时间
//        long nextEventTime = System.currentTimeMillis();
        // 事件循环event loop开始
        while(true){
            beforeSleep();
//            // 记录当前时间
//            long now = System.currentTimeMillis();
//            long timeout = 0;
//            if (now < nextEventTime){
//                /**
//                 * 还未到达下次公事的时间
//                 * 计算：超时时间 = 下次执行公事时间 - 当前时间
//                 */
//                timeout = nextEventTime - now;
//            }else if (now > nextEventTime){
//                /**
//                 * 已经到达下次公事时间
//                 * 1. 执行公事
//                 * 2. 计算：下次公事时间 = 当前时间 + 公事间隔
//                 */
//                nextEventTime = now + 100;
//            }

            // 优化后的逻辑
            long now = System.currentTimeMillis();
            long timeout = eventTime - now;

            // 防止timeout为0导致cpu空转
            if (timeout <= 0){
                timeout = 1;
            }
            // 在容忍时间(timeout)里处理请求事件
            aeProcessEvents(timeout);

            // 当前时间比下一次处理公共事件还要大那么就处理公事
            if (now >= eventTime){
                serverCron();
                eventTime = now + 1000 / hz;
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
            // System.out.println("处理事件:" + key);

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
        // System.out.println("读取数据" +  new String(buffer.array()));

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
        }else if (client.returnValue instanceof ErrorObject){
            client.outBuf = RespUtil.formatError(((ErrorObject) client.returnValue).message);
        }else if (client.returnValue instanceof ArrayObject) {
            ArrayObject retValue = (ArrayObject) client.returnValue;
            client.outBuf = RespUtil.formatArray(retValue.elements);
        }

        // ================================================

        // 将字节数组包装成 Buffer，因为 NIO 的 Channel 只认 Buffer
        ByteBuffer buffer = ByteBuffer.wrap(client.outBuf);
        try {
            // 记录本次成功写入的长度
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
                // 此处做二次判断是为了性能上的优化
                // 1: 如果网络极其拥塞一个字节也没发出去, 那么如果不加此判断就会导致创建一个新的内存块影响性能
                // 2: 同时二次判断截取数组是为了对齐已发送的数据和剩余未发送的数据
                if (totalWritten < client.outBuf.length) {
                    client.outBuf = Arrays.copyOfRange(
                            client.outBuf, totalWritten, client.outBuf.length
                    );
                }
                // 数据没有发完确保继续写,所以这里叠加一次写事件
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                client.write = true;
            } else {
                // 状态清零与事件切换
                //client.outBuf = null;
                client.returnValue = null;
                // 取消写事件
                // 用位运算的原因是避免干扰到其他可能存在的状态，只单独取消写事件
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
                Object result = null;
                try{
                    result = processCommand(client, redisRequest);
                }catch (Exception e){
                    result = new ErrorObject("Error Args or Command, Please check your Command!");
                }
                // 如果有返回值才注册写事件
                if (result != null) {
                    // 标记回复状态
                    client.returnValue = result;
                    client.write = true;

                    // 注册写事件
                    SelectionKey key = client.channel.keyFor(selector);
                    // 叠加事件(让一个连接具备多种状态)
                    // eg: 为了不丢失之前事件的状态，需要使用位运算叠加
                    // 如果客户端在发完第一个指令后紧接着发了第二个指令，服务器就会因为不再关注“读”事件而完全听不到，导致请求丢失或延迟
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                }
            } else if (processed == 0) {
                break; // 数据不完整，等待下次读取
            } else {
                //closeClient(client.channel, client.channel.keyFor(selector), client);
                break;
            }
        }
    }

    public static Object lookUpKeyRead(RedisDB redisDB, String key){
        // 惰性删除
        expireIfNeeded(redisDB, key);
        return lookUpKey(redisDB, key);
    }

    // 惰性删除
    private static void expireIfNeeded(RedisDB redisDB, String key) {
        Long expireTime = redisDB.expires.getTTL(key);
        if (expireTime != null && expireTime <= System.currentTimeMillis()){
            redisDB.dict.remove(key);
            redisDB.expires.remove(key);
        }
    }
    // 寻找key对应的value
    private static Object lookUpKey(RedisDB redisDB, String key) {
        RedisObject redisObject = redisDB.dict.getRedisObject(key);

        if (redisObject != null){
            redisObject.lru = System.currentTimeMillis();
            return redisObject.value;
        }
        return null;
    }


    public static Object processCommand(RedisClient redisClient, RedisRequest redisRequest) {
        System.out.println("handle command: " + redisRequest.command + " args: " + redisRequest.args);
        RedisDB selectedDB = redisClient.selectDB;
        // 为了防止连接redis client发送command命令，这里对command进行过滤
        if("command".equalsIgnoreCase(redisRequest.command)){
            return "OK";
        }
        String key = null;
        if (redisRequest.args.size() > 0){
            key = redisRequest.args.get(0);
        }
        String command = redisRequest.command;

        // 如果当前正在事务状态中并且命令不是控制事务本身的命令，则加入事务队列
        if (redisClient.flags == 3 &&
                !command.equalsIgnoreCase(Multi.MULTI) && !command.equalsIgnoreCase(Multi.DISCARD) &&
                !command.equalsIgnoreCase(Multi.WATCH) && !command.equalsIgnoreCase(Multi.EXEC)) {
            Multi.queueMultiCommand(redisClient, redisRequest);
            return "OK";
        }

        // 内存淘汰策略
        if (maxmemory > 0){
            // 每次set的时候都要检查内存
            int retval = freeMemoryIfNeeded();
            if (retval == -1 && isCmdDenyoom(redisRequest.command)){
                // 等于-1代表失败
                return new ErrorObject("OOM Command not allowed when used mempry > 'maxmemory'.");
            }
        }

        return call(redisClient, redisRequest, selectedDB, key);
    }

    private static int freeMemoryIfNeeded() {
        long dbSize = getDbSize();
        if (dbSize < maxmemory){
            // 内存充足
            return 0;
        }

        // 内存不足并且内存淘汰策略是 noeviction的情况下直接返回-1抛出异常
        if (maxmemory_policy == REDIS_MAXMEMORY_NO_EVICTION){
            return -1;
        }

        // 计算要淘汰多少内存
        long mem_toFree = dbSize - maxmemory;
        // 已经释放的内存
        int mem_freed = 0;



        while (mem_freed < mem_toFree){
            // 是否有过期键删除
            boolean key_freed = false;
            for (int i = 0; i < redisDB.length; i ++){
                // 目标字典
                Dict targetDic = null;
                // 要删除的键
                String deleteKey = null;

                // 根据内存淘汰策略的不同来选择目标字典
                if (maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU || maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM){
                    targetDic = redisDB[i].dict;
                }else {
                    targetDic = redisDB[i].expires;
                }

                if (targetDic.getDictSize() == 0){
                    continue;
                }

                // Random策略
                if (maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM || maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_RANDOM){
                    deleteKey = dictGetRandomKey(targetDic);
                }
                // LRU策略
                if (maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU || maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU){
                    // 从一批sample中选取IDEL最长的key(IDEL = now - lru), IDEL越大代表越久没有被使用
                    int sampleCount = 5; // 采样数量
                    String bestKey = null; // 最佳键
                    long maxIDEL = -1;  // 最大IDEL(
                    for (int j = 0; j < sampleCount; j ++){
                        String key = dictGetRandomKey(targetDic);
                        if (key == null){
                            continue;
                        }

                        Long idel = redisDB[i].dict.getIDLE(key);
                        // 找到IDEL最大的key
                        if (idel > maxIDEL){
                            maxIDEL = idel;
                            bestKey = key;
                        }
                    }
                    // 将bestKey赋值给deleteKey
                    if (bestKey != null){
                        deleteKey = bestKey;
                    }
                }
                // TTL策略
                if (maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_TTL){
                    // 随机从5个key中选取一个最小ttl的key, 并不是把所有的键的ttl进行排序
                    int sampleCount = 5;
                    String tempKey = null;
                    long tempTTL = Long.MAX_VALUE;
                    for (int j = 0; j < sampleCount; j ++){
                        String randomKey = dictGetRandomKey(targetDic);
                        long ttl = redisDB[i].expires.getTTL(randomKey);
                        if (ttl < tempTTL){
                            tempTTL = ttl;
                            tempKey = randomKey;
                        }
                    }
                    deleteKey = tempKey;
                }

                if (deleteKey != null){
                    System.out.println("内存淘汰,策略: " + maxmemory_policy + " 删除key: " + deleteKey);
                    // 删除key
                    key_freed = true;
                    redisDB[i].dict.remove(deleteKey);
                    redisDB[i].expires.remove(deleteKey);
                    mem_freed += 1;
                }

            }

            if (!key_freed){
                // 如果循环结束了, 但是没有满足策略的key
                return -1;
            }
        }


        return mem_freed;
    }

    // 随机获取一个key
    public static String dictGetRandomKey(Dict dict) {
        if (dict.getDictSize() == 0){
            return null;
        }
        List<String> keysArray = new ArrayList<>();

        if (dict.rehash == -1){
            // 如果没有正在进行渐进式哈希那么就直接使用h[0]
            keysArray = new ArrayList<>(dict.ht[0].keySet());
        }else {
            // 如果正在进行渐进式哈希那么就使用h[0]+h[1]
            keysArray = new ArrayList<>(dict.ht[0].keySet());
            List<String> keysArray1 = new ArrayList<>(dict.ht[1].keySet());
            keysArray.addAll(keysArray1);
        }

        Random random = new Random();
        int index = random.nextInt(keysArray.size());
        return keysArray.get(index);
    }

    static long getDbSize(){
        long size = 0;
        for (int i = 0; i < redisDB.length; i ++ ){
            size += redisDB[i].dict.getDictSize();
        }
        return size;
    }

    public static boolean isCmdDenyoom(String command) {
        return "set".equalsIgnoreCase(command) || "setnx".equalsIgnoreCase(command);
    }

    public static Object call(RedisClient redisClient, RedisRequest redisRequest, RedisDB selectedDB, String key) {
        if ("get".equalsIgnoreCase(redisRequest.command)){
            // String key = redisRequest.args.get(0);
            return lookUpKeyRead(selectedDB, key);
        }
        if("set".equalsIgnoreCase(redisRequest.command)){
            // 处理set命令
            RedisObject redisObject = new RedisObject(redisRequest.args.get(1));
            redisClient.selectDB.dict.set(redisRequest.args.get(0), redisObject);
            Multi.touchWatchedKeys(redisClient, redisRequest);
            return "OK";
        }
        if ("select".equalsIgnoreCase(redisRequest.command)){
            // 处理select命令
            int dbIndex = Integer.parseInt(redisRequest.args.get(0));
            if (dbIndex < 0 || dbIndex >= redisDB.length){
                return "ERR invalid DB index";
            }
            redisClient.selectDB = redisDB[dbIndex];
            return "OK";
        }
        if ("expire".equalsIgnoreCase(redisRequest.command)){
            // String key = redisRequest.args.get(0);
            // 如果key不存在那么就返回0
            RedisObject object = selectedDB.dict.getRedisObject(key);
            if (object == null){
                return 0;
            }
            // 参数中是偏移事件
            Long offsetTime = Long.parseLong(redisRequest.args.get(1));
            Long expireTime = System.currentTimeMillis() + offsetTime * 1000;

            selectedDB.expires.set(key, expireTime);
            return 1;
        }
        if ("auth".equalsIgnoreCase(redisRequest.command)){
            return "OK";
        }
        if ("ping".equalsIgnoreCase(redisRequest.command)){
            return "PONG";
        }
        if ("info".equalsIgnoreCase(redisRequest.command)){
            return infoResponse;
        }
        if ("hello".equalsIgnoreCase(redisRequest.command)){
            ErrorObject errorObject = new ErrorObject("ERR unknown command '" + redisRequest.command + "'");
            return errorObject;
        }
        if ("ttl".equalsIgnoreCase(redisRequest.command)){
            Long ttl = selectedDB.expires.getTTL(key);
            if (ttl == null){
                return -1;
            }
            long l = (ttl - System.currentTimeMillis()) / 1000;
            return Long.valueOf(l);
        }
        if ("keys".equalsIgnoreCase(redisRequest.command)){
            String pattern = redisRequest.args.get(0);  // keys pattern中的pattrn参数
            List<String> keys = new ArrayList<>();

            for (RedisDB redisDb : redisDB){
                // 判断是否过期
                for (String getKey : redisDb.dict.ht[0].keySet()){
                    if (redisDb.expires.ht[0].get(getKey) != null){
                        // 如果过期了那么就不加入到list中
                        if (redisDb.expires.getTTL(getKey) - System.currentTimeMillis() < 0){
                            continue;
                        }
                    }
                    // System.out.println(redisDb.expires.getTTL(getKey));
                    if (isMatch(getKey, pattern)){
                        keys.add(getKey);
                    }
                }
                if (redisDb.dict.rehash != -1){
                    for (String getKey : redisDb.dict.ht[1].keySet()){
                        if (redisDb.expires.ht[1].get(getKey) != null){
                            // 如果过期了那么就不加入到list中
                            if (redisDb.expires.getTTL(getKey) - System.currentTimeMillis() < 0){
                                continue;
                            }
                        }
                        if (isMatch(getKey, pattern)){
                            keys.add(getKey);
                        }
                    }
                }
            }
            return keys.toString();
        }
        if ("multi".equalsIgnoreCase(redisRequest.command)){
            // 开启事务
            return Multi.multi(redisClient);
        }
        if ("watch".equalsIgnoreCase(redisRequest.command)){
            return Multi.watch(redisClient, redisRequest);
        }
        if ("unwatch".equalsIgnoreCase(redisRequest.command)){
            return Multi.unwatch(redisClient);
        }
        if ("exec".equalsIgnoreCase(redisRequest.command)){
            // 执行事务
            return Multi.exec(redisClient, selectedDB,  key);
        }
        if ("discard".equalsIgnoreCase(redisRequest.command)){
            return Multi.discard(redisClient);
        }
        if ("subscribe".equalsIgnoreCase(redisRequest.command)){
            // 订阅功能
            for (String arg : redisRequest.args) {
                // 添加channel到redisClient中
                redisClient.subscribedChannels.add(arg);
                List<RedisClient> clientList = pubsub_Channels.computeIfAbsent(arg, k -> new ArrayList<>());
                // 添加当前客户端到订阅者
                if (!clientList.contains(redisClient)){
                    clientList.add(redisClient);
                }else {
                    return "ERR already subscribed";
                }
            }
            return new ArrayObject("subscribe", redisRequest.args.get(0), 1);
        }
        if ("publish".equalsIgnoreCase(redisRequest.command)){
            // 发布功能
            String channel = redisRequest.args.get(0);
            String message = redisRequest.args.get(1);
            List<RedisClient> redisClients = pubsub_Channels.get(channel);
            for (RedisClient client : redisClients) {
                client.returnValue = message;
                client.write = true;

                // 注册可写事件
                SelectionKey selectionKey = client.channel.keyFor(selector);
                selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
            }

            // 返回收到消息的客户端数
            return Long.valueOf(redisClients.size()).toString();
        }
        if ("lpush".equalsIgnoreCase(redisRequest.command)){
            RedisObject redisObject = selectedDB.dict.getRedisObject(key);
            if (redisObject != null && redisObject.type != RedisConstants.REDIS_LIST){
                return new ErrorObject("WRONG TYPE Operation against a key holding the wrong kind of value");
            }
            if (redisObject == null){
                ZipList zipList = new ZipList();
                redisObject = new RedisObject(zipList);
                redisObject.type = RedisConstants.REDIS_LIST;
                redisObject.encoding = RedisConstants.REDIS_ENCODING_ZIPLIST;
                selectedDB.dict.set(key, redisObject);
            }
//            if (redisObject == null){
//                LinkedList zipList = new LinkedList();
//                redisObject = new RedisObject(zipList);
//                redisObject.type = RedisConstants.REDIS_LIST;
//                redisObject.encoding = RedisConstants.REDIS_ENCODING_LINKEDLIST;
//                selectedDB.dict.set(key, redisObject);
//            }
            int count = 0;
            for (String value : redisRequest.args.subList(1, redisRequest.args.size())) {
                listTypePush(redisObject, value, true);
                count ++;
            }
            return Long.valueOf(count).toString();
        }
        if ("lrange".equalsIgnoreCase(redisRequest.command)){
            long start = Long.parseLong(redisRequest.args.get(1));
            long end = Long.parseLong(redisRequest.args.get(2));

            RedisObject redisObject = selectedDB.dict.getRedisObject(key);
            if (redisObject != null && redisObject.type != RedisConstants.REDIS_LIST){
                return new ErrorObject("WRONG TYPE Operation against a key holding the wrong kind of value");
            }
            if (redisObject == null){
                return new ArrayObject();
            }
            if (redisObject.encoding == RedisConstants.REDIS_ENCODING_ZIPLIST){
                ZipList zipList = (ZipList) redisObject.value;
                List<String> list = zipList.range((int)start, (int)end);
                return new ArrayObject(list);
            }else if (redisObject.encoding == RedisConstants.REDIS_ENCODING_LINKEDLIST){
                LinkedList linkedList = (LinkedList) redisObject.value;

                List<Object> range = new ArrayList<>();
                // 如果是-1那么就代表查询整个linkedList
                if (end == -1){
                    end = linkedList.size() - 1;
                }
                for (int i = 0; i < linkedList.size(); i ++ ){
                    // 在start和end之间
                    if (i >= start && i <= end){
                        Object linkedListValue = linkedList.get(i);
                        range.add(linkedListValue);
                    }
                }
                // System.out.println(range.toArray());
                return new ArrayObject(range.toArray());
            }
        }
        if ("blpop".equalsIgnoreCase(redisRequest.command)){
            String s = redisRequest.args.get(1);
            long timeout = Long.parseLong(s);
            RedisObject redisObject = selectedDB.dict.getRedisObject(key);

            if (redisObject != null && redisObject.type != RedisConstants.REDIS_LIST){
                return new ErrorObject("WRONG TYPE Operation against a key holding the wrong kind of value");
            }

            if (redisObject != null){

            }
            if (redisObject == null){
                blockForKeys(redisClient, key, timeout);
                return null;
            }
        }
        return "ERR unknown command '" + redisRequest.command + "'";
    }

    private static void blockForKeys(RedisClient redisClient, String key, long timeout) {
        redisClient.flags = 4;
        redisClient.bpop = new RedisClient.BlockingState();
        redisClient.bpop.timeout = System.currentTimeMillis() + timeout * 1000;
        redisClient.bpop.keys.add(key);

    }

    private static void listTypePush(RedisObject redisObject, String value, boolean isHead) {
        // 如果zipList超过了长度那么就转换为LinkedList
        listTypeConversion();
        if (redisObject.encoding == RedisConstants.REDIS_ENCODING_ZIPLIST){
            // 如果类型是压缩列表
            ZipList zipList = (ZipList) redisObject.value;
            if (isHead){
                zipList.insertFromHead(value);
            }else {
                zipList.insertFromTail(value);
            }

        }else if (redisObject.encoding == RedisConstants.REDIS_ENCODING_LINKEDLIST){
            // 如果类型是链表
            LinkedList linkedList = (LinkedList) redisObject.value;
            if (isHead){
                // 直接插入头部
                linkedList.add(0,  value);
            }else {
                // 从尾部插入
                linkedList.add(value);
            }
        }else{
            System.out.println("listTypePush error");
        }
    }

    private static void listTypeConversion() {
    }

    public static boolean isMatch(String s, String p) {
        // s表示被匹配的字符串str
        int sLen = s.length();
        // p表示匹配字符串s*
        int pLen = p.length();

        // dp[i][j] 表示：s 的前 i 个字符是否与 p 的前 j 个字符匹配
        boolean[][] dp = new boolean[sLen + 1][pLen + 1];

        // 基础情况：两个空字符串匹配
        dp[0][0] = true;

        // 处理模式p开头是连续*的情况：*可以匹配空字符串
        for (int j = 1; j <= pLen; j++) {
            if (p.charAt(j - 1) == '*') {
                dp[0][j] = dp[0][j - 1]; // 当前状态依赖于前一个状态
            } else {
                // 遇到非'*'字符，后续不可能再匹配空字符串，直接跳出循环
                break;
            }
        }
        // 填充dp数组
        for (int i = 1; i <= sLen; i++) {
            for (int j = 1; j <= pLen; j++) {
                char charOfP = p.charAt(j - 1);

                if (charOfP == '*') {
                    // 当遇到'*'时，有两种情况可以使dp[i][j]为true：
                    // 1. 忽略'*'（即*匹配空串）：dp[i][j-1]
                    // 2. 使用'*'匹配当前字符s[i-1]，并继续尝试用这个'*'匹配s中更前面的字符：dp[i-1][j]
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                } else {
                    // 当字符精确匹配，或模式中是'?'时，当前字符匹配成功
                    // 并且前面的子串也需匹配成功
                    if (charOfP == '?' || charOfP == s.charAt(i - 1)) {
                        dp[i][j] = dp[i - 1][j - 1];
                    }
                    // 否则，dp[i][j]保持默认的false
                }
            }
        }

        return dp[sLen][pLen];
    }

    /**
     * 客户端请求信息
     */
    static class RedisRequest{
        String command;
        List<String> args;
    }

    private static void closeClient(SocketChannel socketChannel, SelectionKey key, RedisClient redisClient) throws IOException {
        // 发布订阅的清除
        // 首先从redisClient中获取自己订阅了哪些频道
        List<String> subscribedChannels = redisClient.subscribedChannels;
        if (redisClient.subscribedChannels != null && redisClient.subscribedChannels.size() > 0) {
            for (String channelName : subscribedChannels){
                // 遍历这些频道的名称并清除list中的对应的redisClient
                List<RedisClient> redisClients = pubsub_Channels.get(channelName);
                if (redisClients != null && redisClients.size() > 0) {
                    redisClients.remove(redisClient);
                }
            }
        }
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

        activeExpireCycle(false);
        clientsCron();
    }

    /**
     * 解决客户端阻塞函数
     */
    private static void clientsCron() {
        for (RedisClient redisClient : clients){
            if (redisClient.flags == 4){
                if (redisClient.bpop != null && redisClient.bpop.timeout <= System.currentTimeMillis()){
                    // 如果已经超时了那么就注册写事件
                    redisClient.write = true;
                    redisClient.returnValue = null;

                    SelectionKey selectionKey = redisClient.channel.keyFor(selector);
                    selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
                }
            }
        }
    }

    // 过期键的主动删除 新版, 更贴合redis源码
    public static void activeExpireCycle(boolean flag) {
        // --- 1. 参数初始化 (对应 Redis 源码中的 timelimit) ---
        // 慢模式：25ms (Redis默认值)；快模式：1ms (Redis默认值)
        // 原本设置的 50ms/10ms 对 Redis 来说太久了，会导致明显的命令卡顿
        long timelimit = flag ? 1 : 25; // 这里的单位我们内部逻辑处理为微秒级更精确，但此处保持毫秒逻辑
        long start = System.currentTimeMillis();
        long endTime = start + timelimit;

        // 每次随机采样的数量 (ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP)
        int max_samples = 20;
        long sum = 0;
        long maxSum = flag ? 100 : 1000; // 快模式更谨慎，删除量调低

        // --- 2. 数据库轮询 (避免每次都从 0 号库开始) ---
        // 真实 Redis 会记录上一次清理到哪个 DB 了，这里我们简单处理
        for (int i = 0; i < redisDB.length; i++) {
            RedisDB db = redisDB[i];

            // 如果过期字典为空，直接跳过
            if (db.expires.getDictSize() == 0) continue;

            // --- 3. 核心随机抽样循环 ---
            do {
                // 本轮循环删除过期键的数量
                int expired_this_loop = 0;
                // 采样数量, 如果剩余的键数小于max_samples，则全部遍历, 所以这里取两者最小值
                int num_to_check = Math.toIntExact(Long.valueOf(Math.min(db.expires.getDictSize(), max_samples)));

                for (int j = 0; j < num_to_check; j++) {
                    // 重点：必须使用 getRandomKey()，禁止 entrySet()
                    // 随机获取一个key
                    String key = dictGetRandomKey(db.expires);
                    if (key == null) break;

                    long now = System.currentTimeMillis();
                    Long expireTime = db.expires.getTTL(key);

                    if (expireTime != null && expireTime <= now) {
                        // 记录日志会严重拖慢清理速度，建议仅在调试时开启
                        // RedisObject redisObject = db.dict.getRedisObject(key);
                        System.out.println("过期键的主动删除: " + key);
                        db.dict.remove(key);
                        db.expires.remove(key);
                        sum++;
                        expired_this_loop++;
                    }

                    // 检查时间限制 (每 16 次操作检查一次，减少系统调用开销)
                    if ((sum & 15) == 0) {
                        if (System.currentTimeMillis() >= endTime) return;
                    }
                }

                /**
                 * 4. 概率性退出机制 (Redis 的核心精髓)
                 * 如果这一批次中过期的 Key 比例小于 25%，说明该 DB 中过期键密度不高。
                 * 此时没必要继续浪费 CPU，直接跳出 do-while 去看下一个 DB。
                 */
                if (expired_this_loop <= max_samples / 4) {
                    break;
                }

                // 检查数量上限和时间上限
                if (sum >= maxSum || System.currentTimeMillis() >= endTime) {
                    return;
                }

            } while (db.expires.getDictSize() > 0);
        }
    }

    // 过期键的主动删除
//    public static void activeExpireCycle(boolean flag) {
//        /**
//         * 1. 要控制这个函数需要跑多久
//         * 2. 要控制删除的键的数量
//         */
//
//        // 设置函数最多运行50ms, 删除键的数量最多为10000个
//        long endTime = System.currentTimeMillis() + 50;
//        long maxSum = 10000;
//
//        // 快模式
//        if (flag){
//            endTime = System.currentTimeMillis() + 10;
//            maxSum = 1000;
//        }
//        // 统计删除key的数量
//        long sum = 0;
//
//        for (RedisDB redisDB : redisDB){
//            // 统计key的数量
//            long dbSize = redisDB.dict.getDictSize();
//            if (dbSize == 0){
//                continue;
//            }
//
//
//            for (Map.Entry<String, Long> entry : redisDB.expires.ht[0].entrySet()) {
//                String key = entry.getKey();
//                long expireTime = entry.getValue();
//
//                // 判断是否过期
//                if (expireTime <= System.currentTimeMillis()){
//                    RedisObject redisObject = redisDB.dict.getRedisObject(key);
//                    System.out.println("过期键主动淘汰key: " + key + " value: " + (redisObject == null ? "null" : redisObject));
//
//                    redisDB.dict.remove(key);
//                    redisDB.expires.remove(key);
//                    sum ++;
//                }
//                // 如果过期时间到了或者删除键的数量到达最大值那么就直接返回
//                if (System.currentTimeMillis() >= endTime || sum >= maxSum){
//                    return;
//                }
//            }
//        }
//
//    }

    /**
     * 返回错误信息
     */
    static class ErrorObject {
        String message;

        ErrorObject(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    static String infoResponse = "# Server\r\n" +
            "redis_version:7.0.0\r\n" +
            "redis_mode:standalone\r\n" +
            "os:Linux 5.4.0 x86_64\r\n" +
            "arch_bits:64\r\n" +
            "multiplexing_api:epoll\r\n" +
            "process_id:12345\r\n" +
            "run_id:abc123def456\r\n" + // 模拟运行ID
            "tcp_port:6379\r\n" +
            "uptime_in_seconds:1000\r\n" +
            "uptime_in_days:0\r\n" +
            "hz:10\r\n" +
            "config_file:/path/to/redis.conf\r\n" +
            "\r\n" +
            "# Clients\r\n" +
            "connected_clients:1\r\n" +
            "client_recent_max_input_buffer:2\r\n" +
            "blocked_clients:0\r\n" +
            "\r\n" +
            "# Memory\r\n" +
            "used_memory:1048576\r\n" +
            "used_memory_human:1.00M\r\n" +
            "used_memory_rss:2097152\r\n" +
            "used_memory_peak:2097152\r\n" +
            "used_memory_peak_perc:50.00%\r\n" +
            "mem_fragmentation_ratio:2.00\r\n" +
            "maxmemory:0\r\n" +
            "maxmemory_policy:noeviction\r\n" +
            "mem_allocator:jemalloc-5.2.1\r\n" +
            "\r\n" +
            "# Persistence\r\n" +
            "loading:0\r\n" +
            "rdb_changes_since_last_save:0\r\n" +
            "rdb_bgsave_in_progress:0\r\n" +
            "aof_enabled:0\r\n" +
            "\r\n" +
            "# Stats\r\n" +
            "total_connections_received:5\r\n" +
            "total_commands_processed:100\r\n" +
            "instantaneous_ops_per_sec:0\r\n" +
            "rejected_connections:0\r\n" +
            "keyspace_hits:50\r\n" +
            "keyspace_misses:10\r\n" +
            "\r\n" +
            "# Replication\r\n" +
            "role:master\r\n" +
            "connected_slaves:0\r\n" +
            "master_repl_offset:0\r\n" +
            "\r\n" +
            "# CPU\r\n" +
            "used_cpu_sys:10.5\r\n" +
            "used_cpu_user:20.3\r\n" +
            "\r\n" +
            "# Keyspace\r\n" +
            "db0:keys=10,expires=0,avg_ttl=0\r\n";
}
