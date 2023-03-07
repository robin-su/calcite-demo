package com.calcite.demo.redis;

import java.util.Arrays;

public enum RedisDataFormat {

    RAW("raw"),

    CSV("csv"),

    JSON("json");


    private final String typeName;

    RedisDataFormat(String typeName) {
        this.typeName = typeName;
    }

    public static RedisDataFormat fromTypeName(String name) {
       return Arrays.stream(RedisDataFormat.values())
                .filter(format -> format.getTypeName().equalsIgnoreCase(name))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Invalid DataFormat name:%s",name)));
    }

    public String getTypeName() {
        return typeName;
    }
}
