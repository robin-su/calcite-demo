package com.calcite.demo.redis;

import java.net.URL;
import java.sql.*;

public class RedisTestMain {

    private static final String SQL = "select * from \"stu_01\"";

    public static void main(String[] args) throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("redismodel.json");
        Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath());
        Statement st = connection.createStatement();
        ResultSet resultSet = st.executeQuery(SQL);
        while (resultSet.next()) {
            System.out.println(resultSet.getObject("name"));
            System.out.println(resultSet.getObject("score"));
            System.out.println("----------------");
        }

    }

}
