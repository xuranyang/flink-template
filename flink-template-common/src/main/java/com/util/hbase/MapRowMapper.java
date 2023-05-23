package com.util.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

public class MapRowMapper implements RowMapper<Map<String, String>> {
    @Override
    public Map<String, String> mapRow(Result result, int rowNum) throws Exception {
        Map<String, String> resultMap = new HashMap<>();

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();

        // key为cf
        map.forEach((columnFamily, columnValues) -> {
            // key为column
            columnValues.forEach((column, values) -> {
                byte[] firstValue = values.firstEntry().getValue();
                // cf+column
                resultMap.put(Bytes.toString(columnFamily) + ":" + Bytes.toString(column), Bytes.toString(firstValue));
            });
        });

        return resultMap;
    }
}
