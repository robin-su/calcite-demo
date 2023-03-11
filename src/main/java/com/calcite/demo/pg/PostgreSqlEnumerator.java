package com.calcite.demo.pg;

import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Enumerator;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PostgreSqlEnumerator implements Enumerator<Object> {

    private ResultSet rs;
    private List<Map.Entry<String,Class<?>>> fields;

    public PostgreSqlEnumerator(ResultSet rs, List<Map.Entry<String, Class<?>>> fields) {
        this.rs = rs;
        this.fields = fields;
    }

    @SneakyThrows
    @Override
    public Object current() {
        if(fields.size() == 1) {
            return rs.getObject(1);
        }

        List<Object> row = new ArrayList<Object>();
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            row.add(rs.getObject(i));
        }
        return row;
    }

    @SneakyThrows
    @Override
    public boolean moveNext() {
        return rs.next();
    }

    @SneakyThrows
    @Override
    public void reset() {
        rs.relative(0);
    }

    @SneakyThrows
    @Override
    public void close() {
        rs.getStatement().getConnection().close();
    }
}
