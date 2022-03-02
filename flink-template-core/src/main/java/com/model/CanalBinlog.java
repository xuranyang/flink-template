package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CanalBinlog {
    List<Map<String, String>> data;
    String database;
    Long es;
    Long id;
    Boolean isDdl;
    Map<String, String> mysqlType;
    List<Map<String, String>> old;
    List<String> pkName;
    String sql;
    Map<String, Integer> sqlType;
    String table;
    String ts;
    String type;
}
