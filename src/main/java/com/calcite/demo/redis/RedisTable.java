package com.calcite.demo.redis;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 表元数据内部信息的实际逻辑
 */
public class RedisTable extends AbstractTable
        implements ScannableTable {

    final RedisSchema schema;
    final String tableName;
    final RelProtoDataType protoRowType;
    final ImmutableMap<String,Object> allFields;
    final String dataFormat;
    final RedisConfig redisConfig;

    public RedisTable(RedisSchema schema,
                      String tableName,
                      RelProtoDataType protoRowType,
                      Map<String, Object> allFields,
                      String dataFormat,
                      RedisConfig redisConfig) {
        this.schema = schema;
        this.tableName = tableName;
        this.protoRowType = protoRowType;
        this.allFields = allFields == null ? ImmutableMap.of() : ImmutableMap.copyOf(allFields);
        this.dataFormat = dataFormat;
        this.redisConfig = redisConfig;
    }

    /**
     * 该方法用于返回一个迭代器，该迭代器中封装了所有返回的数据，每屌用一次next返回一行数据。
     * 这也是火山模型中数据调用方式的体现
     *
     * @param dataContext
     * @return
     */
    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {

        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new RedisEnumerator(redisConfig, schema, tableName);
            }
        };
    }

    /**
     * 获取列名和列类型列表
     *
     * @param typeFactory
     * @return
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        // 判断数据类型是否为空
        if(protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        final List<RelDataType> types = new ArrayList<RelDataType>(allFields.size());
        ArrayList<String> names = new ArrayList<>(allFields.size());
        // 遍历所有字段的信息，对每个数据类型进行转换
        for (Object key : allFields.keySet()) {
            final RelDataType type = typeFactory.createJavaType(allFields.get(key).getClass());
            names.add(key.toString());
            types.add(type);
        }
        // 最终对上述整理过的数据类型进行组装并回传
        return typeFactory.createStructType(Pair.zip(names,types));
    }

    static Table create(
            RedisSchema schema,
            String tableName,
            RedisConfig redisConfig,
            RelProtoDataType protoRowType) {
        RedisTableFieldInfo tableFieldInfo = schema.getTableFieldInfo(tableName);
        Map<String, Object> allFields = RedisEnumerator.deduceRowType(tableFieldInfo);
        return new RedisTable(schema, tableName, protoRowType,
                allFields, tableFieldInfo.getDataFormat(), redisConfig);
    }

    static Table create(
            RedisSchema schema,
            String tableName,
            Map operand,
            RelProtoDataType protoRowType) {
        RedisConfig redisConfig = new RedisConfig(schema.host, schema.port,
                schema.database, schema.password);
        return create(schema, tableName, redisConfig, protoRowType);
    }
}
