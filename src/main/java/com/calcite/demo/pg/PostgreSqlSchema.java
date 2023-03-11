package com.calcite.demo.pg;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class PostgreSqlSchema extends AbstractSchema {

    private Map<String, Table> tableMap;

    public PostgreSqlSchema(Map<String, Table> tableMap) {
        this.tableMap = tableMap;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }
}
