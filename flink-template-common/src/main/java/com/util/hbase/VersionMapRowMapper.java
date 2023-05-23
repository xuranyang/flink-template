package com.util.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;


public class VersionMapRowMapper implements RowMapper<Map<String,Map<Long,String>>>{
    public static boolean isEmpty(Map<?, ?> map) {
        return null == map || map.isEmpty();
    }

    @Override
    public Map<String, Map<Long, String>> mapRow(Result result, int rowNum) throws Exception {
        Map<String,Map<Long,String>> resultMap = new HashMap<>();
        // key是列族cf, value是map(k=column,v=各个版本的value map)
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        if (isEmpty(map)) {
            return resultMap;
        }

        // key为cf
        map.forEach((columnFamily, columnValues) -> {
            // key为column
            columnValues.forEach((column, values) -> {
                HashMap<Long, String> versionMap = new HashMap<>();
                values.forEach((timestamp,value)->{
                    versionMap.put(timestamp,Bytes.toString(value));
                });
                // cf+column : {ts:value}
                resultMap.put(Bytes.toString(columnFamily) + ":" + Bytes.toString(column), versionMap);
            });
        });

        return resultMap;
    }
}
