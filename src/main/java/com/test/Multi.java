package com.test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 事务的处理
 * 1. MULTI 命令开启事务
 * 2. EXEC 命令执行事务
 * 3. DISCARD 命令放弃事务
 * 4. WATCH 命令监控数据
 * 5. UNWATCH 命令取消监控
 */
public class Multi {
    public static final String MULTI = "MULTI";
    public static final String EXEC = "EXEC";
    public static final String DISCARD = "DISCARD";
    public static final String WATCH = "WATCH";
    public static final String UNWATCH = "UNWATCH";

    public static class MultiState{
        LinkedList<RedisServer.RedisRequest> commands = new LinkedList<>();  // 事务队列, 在redis中是用链表实现的
        int count = 0; // 事务计数器
    }

    // 入队
    public static void queueMultiCommand(RedisServer.RedisClient redisClient, RedisServer.RedisRequest redisRequest) {
        if (redisClient.multiState == null) {
            redisClient.multiState = new MultiState();
        }

        redisClient.multiState.commands.add(redisRequest);
        redisClient.multiState.count++;
    }

    // 开启事务
    public static Object multi(RedisServer.RedisClient redisClient){
        if (redisClient.flags == 3){
            return new RedisServer.ErrorObject("ERR MULTI calls can not be nested");
        }
        redisClient.flags = 3;
        return "OK";
    }

    // 执行事务
    public static Object exec(RedisServer.RedisClient redisClient, RedisServer.RedisDB selectedDB, String key){
        if (redisClient.flags != 3){
            return new RedisServer.ErrorObject("ERR EXEC without MULTI");
        }

        // 判断watch key是否被修改
        if (redisClient.flags == 5){
            // 取消事务
            redisClient.flags = 0;
            redisClient.multiState = new MultiState();
            return new RedisServer.ErrorObject("ERR WATCH not supported");
        }

        // 执行命令
        redisClient.multiState.commands.forEach(redisRequest -> {
            RedisServer.call(redisClient, redisRequest, selectedDB, key);
        });

        // 修改flag
        redisClient.flags = 0;
        // 清空命令列表
        redisClient.multiState.commands = new LinkedList<>();
        return "OK";
    }

    // 取消事务
    public static Object discard(RedisServer.RedisClient redisClient){
        redisClient.flags = 0;
        redisClient.multiState.commands = new LinkedList<>();
        redisClient.multiState.count = 0;
        return "OK";
    }

    /**
     * “触碰”一个键，如果这个键正在被某个/某些客户端监视着，
     * 那么这个/这些客户端在执行 EXEC 时事务将失败。
     */
    public static void touchWatchedKeys(RedisServer.RedisClient redisClient, RedisServer.RedisRequest request){
        List<RedisServer.RedisDB.WatchedKeyClient> watchedKeys = redisClient.selectDB.watched_keys;
        if (watchedKeys.size() == 0){
            return;
        }
        // 遍历当前DB中所有正在被监控的key
        for (RedisServer.RedisDB.WatchedKeyClient watchedKeyClient : watchedKeys) {
            // 如果当前传入过来的参数和当前正在被监控的参数相同，则代表这个key已经被修改过了，将flag设置为5
            if (watchedKeyClient.key.equalsIgnoreCase(request.args.get(0))){
                watchedKeyClient.redisClient.flags = 5;
            }
        }
    }

    // 监控
    public static Object watch(RedisServer.RedisClient redisClient, RedisServer.RedisRequest request){
        RedisServer.RedisClient.WatchedKey watchedKey = new RedisServer.RedisClient.WatchedKey();
        RedisServer.RedisDB.WatchedKeyClient watchedKeyClient = new RedisServer.RedisDB.WatchedKeyClient();
        watchedKey.key = request.args.get(0);
        watchedKey.db = redisClient.selectDB.id;

        watchedKeyClient.redisClient = redisClient;
        watchedKeyClient.key = request.args.get(0);
        redisClient.selectDB.watched_keys.add(watchedKeyClient);

        redisClient.watched_keys.add(watchedKey);
        return "OK";
    }

    public static Object unwatch(RedisServer.RedisClient redisClient){
        redisClient.watched_keys.clear();
        return "OK";
    }
}
