package com.calcite.demo.redis;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RedisEnumerator implements Enumerator<Object[]> {

    private final Enumerator<Object[]> enumerator;

    public RedisEnumerator(RedisConfig redisConfig, RedisSchema schema, String tableName) {
        // 获取redis管理者对象
        RedisJedisManager redisJedisManager = new RedisJedisManager(redisConfig.getHost(),
                redisConfig.getPort(),
                redisConfig.getDatabase(),
                redisConfig.getPassword());

        try (Jedis jedis = redisJedisManager.getResource()) {
            if(StringUtils.isNotEmpty(redisConfig.getPassword())) {
                jedis.auth(redisConfig.getPassword());
            }
            // 获取表元数据信息
            RedisTableFieldInfo tableFieldInfo = schema.getTableFieldInfo(tableName);
            RedisDataProcess dataProcess = new RedisDataProcess(jedis, tableFieldInfo);
            List<Object[]> objs = dataProcess.read();
            // 使用Linq4j的接口将这些元素封装成迭代器
            enumerator =  Linq4j.enumerator(objs);
        }
    }

    static Map<String,Object> deduceRowType(RedisTableFieldInfo tableFieldInfo) {
        final Map<String, Object> fieldBuilder = new LinkedHashMap<>();
        String dataFormat = tableFieldInfo.getDataFormat();
        RedisDataFormat redisDataFormat = RedisDataFormat.fromTypeName(dataFormat);
        assert redisDataFormat != null;
        if (redisDataFormat == RedisDataFormat.RAW) {
            fieldBuilder.put("key", "key");
        } else {
            for (LinkedHashMap<String, Object> field : tableFieldInfo.getFields()) {
                fieldBuilder.put(field.get("name").toString(), field.get("type").toString());
            }
        }
        return fieldBuilder;
    }

    /**
     * 获取指针指向的当前元素
     * @return
     */
    @Override
    public Object[] current() {
        return enumerator.current();

    }

    /**
     * 判断是否还有下一个元素，有则返回true，创建完迭代器之后或者调用完reset方法之后，指针指向了第一个元素之前的位置而不是第一个元素。
     * 因此执行的时候最先调用的便是moveNext方法，用于移动指针到第一个元素。当指针移动到末尾之后便会返回false
     *
     * @return
     */
    @Override
    public boolean moveNext() {
        return enumerator.moveNext();
    }

    /**
     * 该方法用于重置迭代器的指针，将其指向第一个元素之前的位置
     */
    @Override
    public void reset() {
        enumerator.reset();
    }

    /**
     * 关闭迭代器相关的资源，幂等的
     */
    @Override
    public void close() {
        enumerator.close();
    }
}
