package com.test;

public class RedisConstants {
    /**
     * 对象类型 (逻辑抽象)
     * 类似于 Java 中的接口，定义了数据的逻辑结构
     */
    public static final int REDIS_STRING = 0;   // 字符串
    public static final int REDIS_LIST = 1;     // 列表
    public static final int REDIS_SET = 2;      // 集合
    public static final int REDIS_ZSET = 3;     // 有序集合
    public static final int REDIS_HASH = 4;     // 哈希

    /**
     * 对象编码 (具体实现)
     * 类似于 Java 中的实现类，定义了数据在内存中的实际存储方式
     * 注意：Redis 3.2+ 已移除 ZIPMAP 和 LINKEDLIST 作为主流实现
     */
    public static final int REDIS_ENCODING_RAW = 0;           // 字符串原始编码 (SDS)
    public static final int REDIS_ENCODING_INT = 1;           // 整数编码 (存储整数值)
    public static final int REDIS_ENCODING_HT = 2;            // 散列表 (哈希表实现)
    public static final int REDIS_ENCODING_ZIPMAP = 3;        //
    public static final int REDIS_ENCODING_LINKEDLIST = 4;    // 链表 (列表实现)
    public static final int REDIS_ENCODING_ZIPLIST = 5;       // 压缩列表 (小列表/小哈希)
    public static final int REDIS_ENCODING_INTSET = 6;        // 整数集合 (小集合)
    public static final int REDIS_ENCODING_SKIPLIST = 7;      // 跳表 (有序集合)
    public static final int REDIS_ENCODING_EMBSTR = 8;        // 嵌入式SDS (小字符串)


    /**
     * 对象类型 (type)  → 逻辑抽象 (如 String, List)
     *   │
     *   ├── 字符串 (REDIS_STRING)
     *   │    ├── REDIS_ENCODING_RAW   (长字符串)
     *   │    ├── REDIS_ENCODING_INT   (整数)
     *   │    └── REDIS_ENCODING_EMBSTR (小字符串)
     *   │
     *   ├── 列表 (REDIS_LIST)
     *   │    ├── REDIS_ENCODING_ZIPLIST ( 小列表 [长度<=64])
     *   │    └── REDIS_ENCODING_LINKEDLIST (长度>64)
     *   │
     *   ├── 集合 (REDIS_SET)
     *   │    └── REDIS_ENCODING_INTSET (小集合，元素为整数)
     *   │
     *   ├── 有序集合 (REDIS_ZSET)
     *   │    ├── REDIS_ENCODING_ZIPLIST (小有序集合：元素数≤128且元素长度≤64字节)
     *   │    └── REDIS_ENCODING_SKIPLIST (大有序集合：元素数>128或元素长度>64字节)
     *   │
     *   └── 哈希 (REDIS_HASH)
     *        ├── REDIS_ENCODING_HT      (大哈希，使用字典)
     *        └── REDIS_ENCODING_ZIPLIST (小哈希：字段数≤512且字段/值长度≤64字节)
     *
     * 注：Redis 3.2+ 已移除 ZIPMAP 和 LINKEDLIST
     *     所有列表对象在客户端看到的编码均为 ZIPLIST (内部实现为 QUICKLIST)
     */


    /**
     * Redis 对象类型与编码类型对应关系表
     *
     * 对象类型     编码类型                  适用条件                              内存效率         读写性能
     * 字符串       REDIS_ENCODING_INT        整数值 (如 set num 100)               ⭐⭐⭐⭐⭐   ⭐⭐⭐⭐⭐
     * 字符串       REDIS_ENCODING_EMBSTR     字符串长度 ≤ 44 字节                  ⭐⭐⭐⭐    ⭐⭐⭐⭐
     * 字符串       REDIS_ENCODING_RAW        字符串长度 > 44 字节                  ⭐⭐      ⭐⭐⭐
     *
     * 列表         REDIS_ENCODING_ZIPLIST    元素数 ≤ 512 且每个元素长度 ≤ 64 字节  ⭐⭐⭐⭐    ⭐⭐⭐
     * 列表         (实际使用 QUICKLIST)      元素数 > 512 或元素长度 > 64 字节      ⭐⭐⭐     ⭐⭐⭐⭐
     *
     * 集合         REDIS_ENCODING_INTSET     所有元素是整数且数量 ≤ 512            ⭐⭐⭐⭐    ⭐⭐⭐⭐
     * 集合         REDIS_ENCODING_HT         元素包含非整数或数量 > 512            ⭐⭐      ⭐⭐⭐⭐
     *
     * 有序集合     REDIS_ENCODING_ZIPLIST    元素数 ≤ 128 且每个元素长度 ≤ 64 字节  ⭐⭐⭐⭐    ⭐⭐⭐
     * 有序集合     REDIS_ENCODING_SKIPLIST   元素数 > 128 或元素长度 > 64 字节      ⭐⭐      ⭐⭐⭐⭐
     *
     * 哈希         REDIS_ENCODING_ZIPLIST    字段数 ≤ 512 且字段/值长度 ≤ 64 字节  ⭐⭐⭐⭐    ⭐⭐⭐
     * 哈希         REDIS_ENCODING_HT         字段数 > 512 或字段/值长度 > 64 字节   ⭐⭐      ⭐⭐⭐⭐
     *
     * 注：Redis 3.2+ 已移除 ZIPMAP 和 LINKEDLIST，所有列表对象在客户端看到的编码均为 ZIPLIST
     * (实际内部使用 QUICKLIST 实现，但编码常量仍为 REDIS_ENCODING_ZIPLIST)
     */

}