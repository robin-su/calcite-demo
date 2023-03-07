package com.calcite.demo.redis;

import com.google.common.base.Preconditions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

/**
 * 构建RedisShcema
 */
public class RedisSchemaFactory implements SchemaFactory {

    /**
     * 创建RedisSchema,并将所有的表信息封装到RedisSchema中
     *
     * @param schemaPlus 封装了父级别的Schema信息
     * @param name Schema 的信息
     * @param operand 从配置文件传来的配置文件信息，model.json文件中的operand
     * @return
     */
    @Override
    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
        Preconditions.checkArgument(operand.get("tables") != null,
                "tables must be specified");
        Preconditions.checkArgument(operand.get("host") != null,
                "host must be specified");
        Preconditions.checkArgument(operand.get("port") != null,
                "port must be specified");
        Preconditions.checkArgument(operand.get("database") != null,
                "database must be specified");
        // 获取所有的表信息
        List<Map<String,Object>> tables = (List)operand.get("tables");
        // 获取主机信息
        String host = operand.get("host").toString();
        // 获取端口
        int port = (int)operand.get("port");
        // 获取数据库信息
        int database = Integer.parseInt(operand.get("database").toString());
        // 获取密码信息
        String password = operand.get("password") == null ? null : operand.get("password").toString();
        // 封装并返回RedisSchema对象
        return new RedisSchema(host,port,database,password,tables);
    }
}
