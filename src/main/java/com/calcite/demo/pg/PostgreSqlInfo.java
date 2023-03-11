package com.calcite.demo.pg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PostgreSqlInfo {

    private String url;
    private String user;
    private String password;

}
