package com.calcite.demo.pg;

import java.net.URL;
import java.sql.*;

public class PostgreMain {

    public static void main(String[] args) throws SQLException {
        String sql = "select \"code\" from PG.films where \"code\" ='movie' limit 2";
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=src/main/resources/pgmodel.json")) {
//            final Statement statement = conn.createStatement();
//            assertTrue(statement.execute("SELECT * FROM films limit 2"));
//            assertTrue(statement.getResultSet().next());
//            assertEquals("movie", statement.getResultSet().getString("code"));

            final Statement statement = conn.createStatement();
            final ResultSet rs = statement.executeQuery(sql);
            final int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columnCount; i++) {
                    System.out.println(rs.getObject(i + 1));
                }
            }
        }
    }
}
