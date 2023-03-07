package com.calcite.demo.redis;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * 创建表的工厂
 */
public class RedisTableFactory implements TableFactory {

    public static final RedisTableFactory INSTANCE = new RedisTableFactory();

    private RedisTableFactory() {
    }

    // name that is also the same name as a complex metric
    @Override
    public Table create(SchemaPlus schema, String tableName, Map operand,
                        @Nullable RelDataType rowType) {
        final RedisSchema redisSchema = schema.unwrap(RedisSchema.class);
        final RelProtoDataType protoRowType =
                rowType != null ? RelDataTypeImpl.proto(rowType) : null;
        return RedisTable.create(redisSchema, tableName, operand, protoRowType);
    }
}
