package com.calcite.demo.redis;

public class RedisConfig {

    private final String host;
    private final int port;
    private final int database;

    private final String password;

    public RedisConfig(String host, int port, int database, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDatabase() {
        return database;
    }

    public String getPassword() {
        return password;
    }
}
