package com.calcite.demo.redis;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class RedisSchema extends AbstractSchema {

    public final String host;
    public final int port;
    public final int database;

    public final String password;

    public final List<Map<String,Object>> tables;

    private Map<String, Table> tableMap = null;

    RedisSchema(String host, int port, int database, String password, List<Map<String, Object>> tables) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.password = password;
        this.tables = tables;
    }

    @Override
    public Map<String, Table> getTableMap() {
        JsonCustomTable[] jsonCustomTables = new JsonCustomTable[tables.size()];
        // 获得所有表名的集合
        Set<String> tableNames = Arrays.stream(tables.toArray(jsonCustomTables))
                .map(e -> e.name).collect(Collectors.toSet());
        // 构造表名、表元数据的映射关系  tableName -> Table
        tableMap = Maps.asMap(
                ImmutableSet.copyOf(tableNames),
                CacheBuilder.newBuilder()
                        .build(CacheLoader.from(this::table)));
        return tableMap;
    }

    /**
     * 根据表名获取表元数据信息
     *
     * @param tableName
     * @return
     */
    private Table table(String tableName) {
        RedisConfig redisConfig = new RedisConfig(host, port, database, password);
        return RedisTable.create(RedisSchema.this, tableName, redisConfig, null);
    }

    public RedisTableFieldInfo getTableFieldInfo(String tableName) {
        RedisTableFieldInfo tableFieldInfo = new RedisTableFieldInfo();
        List<LinkedHashMap<String, Object>> fields = new ArrayList<>();
        Map<String, Object> map;
        String dataFormat = "";
        String keyDelimiter = "";
        for (int i = 0; i < this.tables.size(); i++) {
            JsonCustomTable jsonCustomTable = (JsonCustomTable) this.tables.get(i);
            if (jsonCustomTable.name.equals(tableName)) {
                map = jsonCustomTable.operand;
                if (map.get("dataFormat") == null) {
                    throw new RuntimeException("dataFormat is null");
                }
                if (map.get("fields") == null) {
                    throw new RuntimeException("fields is null");
                }
                dataFormat = map.get("dataFormat").toString();
                fields = (List<LinkedHashMap<String, Object>>) map.get("fields");
                if (map.get("keyDelimiter") != null) {
                    keyDelimiter = map.get("keyDelimiter").toString();
                }
                break;
            }
        }
        tableFieldInfo.setTableName(tableName);
        tableFieldInfo.setDataFormat(dataFormat);
        tableFieldInfo.setFields(fields);
        if (StringUtils.isNotEmpty(keyDelimiter)) {
            tableFieldInfo.setKeyDelimiter(keyDelimiter);
        }
        return tableFieldInfo;
    }
}
