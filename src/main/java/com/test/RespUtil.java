package com.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespUtil {
    /**
     * 解析 Redis 请求命令
     *
     * @param bytes        请求数据
     * @param offset       数据偏移量
     * @param length       数据长度
     * @param redisRequest 解析结果
     * @return 解析成功返回解析的字节数，解析失败返回-1，需要更多数据返回0
     */
    public static int parseCommand(byte[] bytes, int offset, int length, RedisServer.RedisRequest redisRequest) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        try {
            if (buffer.remaining() < 3) return 0; // 至少需要 "*1\r\n"

            if (buffer.get() != '*') return -1; // 协议头错误

            int argCount = parseNumber(buffer); // 解析参数数量
            if (argCount <= 0) return -1;

            // 新增：消费CRLF分隔符
            if (buffer.remaining() < 2 || buffer.get() != '\r' || buffer.get() != '\n') {
                return -1;
            }
            int totalProcessed = 1 + 2; // 已处理'*'和CRLF

            List<String> args = new ArrayList<>(argCount);

            for (int i = 0; i < argCount; i++) {
                if (buffer.remaining() < 3) return 0; // 需要更多数据
                if (buffer.get() != '$') return -1;
                totalProcessed++;

                int paramLen = parseNumber(buffer); // 参数长度
                if (paramLen < 0) return -1;

                // 新增：消费CRLF分隔符
                if (buffer.remaining() < 2 || buffer.get() != '\r' || buffer.get() != '\n') {
                    return -1;
                }
                totalProcessed += 2;

                if (buffer.remaining() < paramLen + 2) return 0; // 数据不完整
                byte[] param = new byte[paramLen];
                buffer.get(param);
                args.add(new String(param, StandardCharsets.UTF_8));
                totalProcessed += paramLen;

                // 消费参数后的CRLF
                if (buffer.remaining() < 2 || buffer.get() != '\r' || buffer.get() != '\n') {
                    return -1;
                }
                totalProcessed += 2;
            }

            if (!args.isEmpty()) {
                redisRequest.command = args.get(0);
                redisRequest.args = args.subList(1, args.size());
            }
            return totalProcessed;
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * 从 ByteBuffer 中解析数字，直到遇到非数字字符
     */
    public static int parseNumber(ByteBuffer buffer) {
        int number = 0;
        try {
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                char c = (char) b;

                if (c < '0' || c > '9') {
                    buffer.position(buffer.position() - 1); // 关键回退：指针回到非数字字符（如\r）的位置
                    break;
                }
                number = number * 10 + (c - '0');
            }
            return number;
        } catch (Exception e) {
            return -1;
        }
    }


    // ============= RESP 协议格式化工具方法 =============
    static byte[] formatSimpleString(String value) {
        return ("+" + value + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    static byte[] formatError(String message) {
        return ("-ERR " + message + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    static byte[] formatInteger(int value) {
        return (":" + value + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    static byte[] formatBulkString(String value) {
        if (value == null) return "$-1\r\n".getBytes(StandardCharsets.UTF_8);

        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        String header = "$" + data.length + "\r\n";
        return (header + value + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    // ============= 数组回复格式化方法 =============
    static byte[] formatArray(Object... elements) {
        if (elements == null) {
            return "*0\r\n".getBytes(StandardCharsets.UTF_8);
        }

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // 写入数组头：*<元素个数>\r\n
            String arrayHeader = "*" + elements.length + "\r\n";
            outputStream.write(arrayHeader.getBytes(StandardCharsets.UTF_8));

            // 遍历并格式化每个元素
            for (Object element : elements) {
                if (element instanceof String) {
                    // 字符串按 Bulk String 处理
                    outputStream.write(formatBulkString((String) element));
                } else if (element instanceof Integer) {
                    // 整数按 Integer 处理
                    outputStream.write(formatInteger((Integer) element));
                } else if (element instanceof byte[]) {
                    // 已经是字节数组的直接写入（支持已格式化的 RESP 数据）
                    outputStream.write((byte[]) element);
                } else if (element == null) {
                    // null 值按 Null Bulk String 处理
                    outputStream.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
                } else {
                    throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                }
            }

            return outputStream.toByteArray();

        } catch (IOException e) {
            // 理论上不会发生，因为 ByteArrayOutputStream 不会抛出 IOException
            throw new RuntimeException("Failed to format array", e);
        }
    }


}
