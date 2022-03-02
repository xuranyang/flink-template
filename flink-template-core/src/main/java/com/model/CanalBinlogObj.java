package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CanalBinlogObj<T> implements Serializable {
    List<T> data;
    String database;
    Long es;
    Long id;
    Boolean isDdl;
    Map<String, String> mysqlType;
    List<T> old;
    List<String> pkName;
    String sql;
    Map<String, Integer> sqlType;
    String table;
    String ts;
    String type;
}
