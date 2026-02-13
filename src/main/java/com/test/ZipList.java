package com.test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZipList {
    // zlend 标记(结束位-1)
    private static final byte ZIPLIST_END = (byte) 0xFF;

    // 内部存储，使用字节数组模拟连续内存
    private byte[] data;
    // 表示当前字符数组占用了多少字节
    private int totalLength; // zlbytes
    // 表示当前字符数组中最后一个entry的开头下标
    private int tailOffset;  // zltail
    // 表示当前字符数组中元素个数
    private int entryCount;  // zllen

    // 创建一个新的空 ziplist
    public ZipList() {
        // 初始化头部：zlbytes(4) + zltail(4) + zllen(2) + zlend(1)
        data = new byte[11];
        totalLength = 11; // 4+4+2+1
        tailOffset = 10;  // zltail 指向 zlend 的位置
        entryCount = 0;

        // 设置 zlbytes, zltail, zllen
        setZlbytes(totalLength);
        setZltail(tailOffset);
        setZllen(entryCount);

        // 设置 zlend
        data[data.length - 1] = ZIPLIST_END;
    }

    // 设置 zlbytes (4字节)
    private void setZlbytes(int value) {
        // 大端序
        data[0] = (byte) (value >> 24);
        data[1] = (byte) (value >> 16);
        data[2] = (byte) (value >> 8);
        data[3] = (byte) value;
    }

    // 设置 zltail (4字节)
    private void setZltail(int value) {
        // 取左边最高的8位
        data[4] = (byte) (value >> 24);
        data[5] = (byte) (value >> 16);
        data[6] = (byte) (value >> 8);
        // 强转只保留右端的8位
        data[7] = (byte) value;
    }

    // 设置 zllen (2字节)
    private void setZllen(int value) {
        data[8] = (byte) (value >> 8);
        data[9] = (byte) value;
    }

    // 添加字符串到 ziplist 末尾
    public void insertFromTail(String s) {
        // 1. 计算空间：不再需要额外的 +1 给结束符 '0'
        // prevlen(4) + encoding(2) + data(s.length)
        int entryLength = 4 + 2 + s.getBytes().length;
        int requiredSpace = entryLength;

        // 2. 扩容逻辑
        if (data.length < totalLength + requiredSpace) {
            byte[] newData = new byte[totalLength + requiredSpace + 10];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }

        // 3. 确定写入位置
        // 第一个元素写在索引 10，后续元素覆盖旧的 zlend (totalLength - 1)
        int writePos = (entryCount == 0) ? 10 : (totalLength - 1);

        // 4. 计算 prevlen
        int prevlen = (entryCount == 0) ? 0 : (writePos - tailOffset);

        // 5. 写入数据
        int entryStartPos = writePos;
        int pos = writePos;

        // 写入 prevlen (4字节)
        pos = writePrevlen(data, pos, prevlen);

        // 写入 encoding (2字节)，内部会存储 s.length()
        pos = writeEncoding(data, pos, s);

        // 写入真实字符串数据
        byte[] strBytes = s.getBytes();
        System.arraycopy(strBytes, 0, data, pos, strBytes.length);
        pos += strBytes.length;

        // 6. 写入新的 zlend (-1)
        data[pos] = (byte) 0xFF;

        // 7. 更新元数据
        this.tailOffset = entryStartPos; // 指向当前 Entry 开头
        this.totalLength = pos + 1;      // 总长度包含最后的 zlend
        this.entryCount++;

        setZlbytes(totalLength);
        setZltail(tailOffset);
        setZllen(entryCount);
    }

    /**
     * 从头部插入字符串（类似 Redis 的 LPUSH 操作）
     * T = O(N) （因为需要移动整个 ziplist 数据）
     */
    public void insertFromHead(String s) {
        // 1. 计算新节点所需空间（包含 prevlen + encoding + entry-data）
        int entryLength = calculateEntryLength(s);
        // 使用字符串的字节数组计算字节长度
        int requiredSpace = 4 + 2 + s.getBytes().length;; // prevlen(4) + entry

        // 2. 计算当前 ziplist 长度（用于后续内存移动）
        int currentLength = totalLength;

        // 3. 扩容（确保有足够的空间）
        if (data.length < currentLength + requiredSpace) {
            byte[] newData = new byte[currentLength + requiredSpace + 10];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }

        // 4.将现有所有 Entry（从索引 10 开始到 zlend 之前）向后移动
        // 原本第一个元素在索引 10
        if (entryCount > 0) {
            // 修正：搬运从索引 10 开始直到当前总长度的所有字节
            System.arraycopy(data, 10, data, 10 + requiredSpace, totalLength - 10);
        }

        // 5. 写入新节点（在头部位置 10(第十一个元素)）
        // pos 用于记录当前写入元素的位置在哪
        int pos = 10;

        // 写入 prevlen = 0（第一个节点没有前置节点）
        pos = writePrevlen(data, pos, 0);

        // 写入 encoding（根据字符串类型）
        pos = writeEncoding(data, pos, s);

        // 写入 entry-data
        pos = writeEntryData(data, pos, s);

        /**
         * 现在旧节点被移动到了10 + requiredSpace的位置，并且前面有节点所以更新它的prevLen
         */
        if (entryCount > 0){
            writePrevlen(data, 10 + requiredSpace, requiredSpace);
        }


        // 6. 更新头部信息
        totalLength += requiredSpace;
        if (entryCount == 0) {
            // 第一次插入，尾部偏移量就是起始点 10
            tailOffset = 10;
        } else {
            // 后续插入，旧的尾巴整体向后挪了 requiredSpace 字节
            tailOffset += requiredSpace;
        }
        data[tailOffset + requiredSpace] = (byte) 0XFF;
        entryCount++;

        setZlbytes(totalLength);
        setZltail(tailOffset);
        setZllen(entryCount);
    }

    // 计算字符串元素所需的长度
    private int calculateEntryLength(String s) {
        // 实际中需要根据字符串长度计算，这里简化
        return s.getBytes().length + 2; // 假设 encoding 占2字节
    }

    // 写入 prevlen
    private int writePrevlen(byte[] data, int pos, int prevlen) {
        // 实际实现中需要处理不同长度的 prevlen
        // 这里简化为写入4字节
        data[pos] = (byte) (prevlen >> 24);
        data[pos + 1] = (byte) (prevlen >> 16);
        data[pos + 2] = (byte) (prevlen >> 8);
        data[pos + 3] = (byte) prevlen;
        return pos + 4;
    }

    // 写入 encoding (固定两个字节的长度)
    private int writeEncoding(byte[] data, int pos, String s) {
        int len = s.getBytes().length;
        // 使用大端序存储长度
        // 提取int最高的8位(2位encoding只能获得int的16位)
        data[pos] = (byte) (len >> 8);
        // 直接取最低8位(使用byte切割)
        data[pos + 1] = (byte) len;
        return pos + 2;
    }

    // 写入 entry-data
    private int writeEntryData(byte[] data, int pos, String s) {
        // 写入字符串内容
        for (int i = 0; i < s.length(); i++) {
            data[pos + i] = (byte) s.charAt(i);
        }
        return pos + s.length();
    }

    // 从尾部遍历
    public String getFromTail(int index) {
        // 1. 检查边界
        if (entryCount == 0 || index < 0 || index >= entryCount) {
            return null;
        }

        // 2. 从表尾偏移量开始（最后一个节点的开头）
        int pos = tailOffset;

        // 3. 循环向前“跳跃”
        for (int i = 0; i < index; i++) {
            // 【核心修正】读取当前节点开头的 4 个字节，这才是记录“前一跳距离”的地方
            // 注意：这里传的是 pos，而不是 pos-4
            int prevlen = readPrevlen(data, pos);

            if (prevlen == 0) {
                // 如果 prevlen 为 0，说明已经跳到第一个节点了，无法再往前
                break;
            }

            // 向上移动到前一个节点的开头
            pos = pos - prevlen;
        }

        // 4. 解析该位置的 Entry
        return parseEntryAtPos(data, pos);
    }

    public String pop(){
        if (entryCount == 0){
            return null;
        }
        int targetPos = tailOffset;

        String result = parseEntryAtPos(data, targetPos);

        // 获取该节点的长度
        // 结构：prevlen(4) + encoding(2) + dataLen
        int dataLen = ((data[targetPos + 4] & 0xFF) << 8) | (data[targetPos + 5] & 0xFF);
        int entryLen = 4 + 2 + dataLen;

        // 将 zlend (0xFF) 挪到当前节点起始处
        data[targetPos] = (byte) 0xFF;

        // 更新元数据
        if (entryCount == 1) {
            // 删掉最后一个元素后，回归初始状态
            tailOffset = 10;
            totalLength = 11;
        } else {
            // 还有剩余元素，tailOffset 需要回溯到上一个节点的开头
            // 我们读取刚刚被删掉位置的 prevlen，它记录了上一个节点的长度
            int prevLen = readPrevlen(data, targetPos);
            tailOffset = targetPos - prevLen;
            totalLength -= entryLen;
        }

        entryCount--;

        // 同步更新 Header
        setZlbytes(totalLength);
        setZltail(tailOffset);
        setZllen(entryCount);

        // 缩容优化：如果实际数据长度远小于数组容量，进行瘦身
        if (data.length > totalLength + 100) { // 比如多出 100 字节就缩容
            byte[] newData = new byte[totalLength];
            System.arraycopy(data, 0, newData, 0, totalLength);
            this.data = newData;
        }
        return result;
    }

    // 修正解析逻辑：现在 pos 确切地指向 Entry 的开头 (prevlen)
    private String parseEntryAtPos(byte[] data, int pos) {
        // 1. 跳过 prevlen (4字节) 得到 encoding 的位置
        int encodingPos = pos + 4;

        // 2. 从 encoding 中读取长度 (2字节)
        // 左移8位后和最右边8位合并
        int len = ((data[encodingPos] & 0xFF) << 8) | (data[encodingPos + 1] & 0xFF);

        // 3. 计算数据开始的位置 (pos + prevlen长度 + encoding长度)
        int dataPos = encodingPos + 2;

        // 4. 根据读到的长度直接截取字符串，既准确又安全
        return new String(data, dataPos, len);
    }

    // 使用 ByteBuffer 的替代实现
    private int readPrevlen(byte[] data, int pos) {
        // 读取data数组从pos字节开始读取4个字节并按大端序转化为int
        return ByteBuffer.wrap(data, pos, 4)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
    }

    /**
     * 获取指定范围的元素（类似 Redis 的 LRANGE 命令）
     *
     * @param start 起始索引（0-based，可为负数表示从尾部开始）
     * @param end   结束索引（0-based，可为负数表示从尾部开始）
     * @return 指定范围的字符串列表（顺序为头部到尾部）
     */

    public List<String> range(int start, int end) {
        List<String> list = new ArrayList<>();
        if (entryCount == 0) return list;

        int pos = 10; // 从第一个 Entry 开始
        // 正向遍历：跳过 prevlen 和 encoding 读数据
        while (pos < totalLength - 1 && data[pos] != ZIPLIST_END) {
            int currentPos = pos;
            // 1. 跳过 prevlen
            pos += 4;
            // 2. 从 encoding 读长度
            int len = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
            pos += 2;
            // 3. 提取数据
            list.add(new String(data, pos, len));
            // 4. 跳到下一个节点开头
            pos += len;
        }

        // 处理 Redis 风格的索引
        int size = list.size();
        // 倒着数
        if (start < 0) start = size + start;
        if (end < 0) end = size + end;
        // 兜底修正,防止崩溃
        start = Math.max(0, start);
        end = Math.min(size - 1, end);

        if (start > end) return new ArrayList<>();
        return list.subList(start, end + 1);
    }

    // 打印 ziplist 内容（用于调试）
    public void print() {
        List<String> range = range(0, entryCount - 1);
        System.out.println("ZipList:" + range.toString());
    }

    public static void main(String[] args) {
        ZipList ziplist = new ZipList();
        ziplist.insertFromTail("1");
        ziplist.insertFromTail("111111111321312421412");
        ziplist.insertFromTail("5");

        ziplist.print();
        List<String> range = ziplist.range(0, 3);

//         System.out.println("Range: " + range.toString());
//         System.out.println((byte)(200) & 0xFF);
    }
}
