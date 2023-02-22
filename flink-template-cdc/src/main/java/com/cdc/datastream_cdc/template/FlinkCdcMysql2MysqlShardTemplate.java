package com.cdc.datastream_cdc.template;

import com.alibaba.fastjson.JSONObject;
import com.cdc.datastream_cdc.FlinkCdcMysql2MysqlShard;
import com.sink.util.JdbcUtil;
import com.util.FlinkUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * CREATE TABLE `cdc_shard_table` (
 * `id` bigint NOT NULL AUTO_INCREMENT,
 * `name` varchar(255) DEFAULT NULL,
 * `age` int DEFAULT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 * <p>
 * <p>
 * CREATE TABLE `cdc_shard_table_0` LIKE `cdc_shard_table`;
 * CREATE TABLE `cdc_shard_table_1` LIKE `cdc_shard_table`;
 * <p>
 * {"database":"flink_test","after_data":{"name":"yyf","id":12,"age":38},"before_data":{},"operation":"CREATE","table":"cdc_shard_table"}
 * {"database":"flink_test","after_data":{},"before_data":{"name":"yyf","id":12,"age":38},"operation":"DELETE","table":"cdc_shard_table"}
 * {"database":"flink_test","after_data":{"name":"yyf","id":12,"age":39},"before_data":{"name":"yyf","id":12,"age":38},"operation":"UPDATE","table":"cdc_shard_table"}
 */
public class FlinkCdcMysql2MysqlShardTemplate {
    private static Logger log = LoggerFactory.getLogger(FlinkCdcMysql2MysqlShard.class);
    private static String OP = "op";
    private static String AFTER_DATA = "after_data";
    private static String BEFORE_DATA = "before_data";
    private static String DATABASE = "database";
    private static String TABLE = "table";
    private static String CREATE = "CREATE";
    private static String DELETE = "DELETE";
    private static String UPDATE = "UPDATE";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        ParameterTool parameterTool = FlinkUtils.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        //show binary logs;
        //show master status;
        String offsetFile = parameterTool.get("offsetFile", "binlog.000042");
        //show binlog events  IN 'mysql-bin.000008' FROM 154 ;
        long offsetPos = parameterTool.getLong("offsetPos", 926L);  //154 219 504
        // 分表取模的字段
        String modField = parameterTool.get("modField", "id");
        String primaryKey = parameterTool.get("primaryKey", "id");
        // 分表取模的数量
        Integer modNum = parameterTool.getInt("modNum", 2);

        String database = parameterTool.get("database", "flink_test");
        String table = parameterTool.get("table", "cdc_shard_table");
        String dbTable = database + "." + table;

        String sourceMysqlConfig = parameterTool.get("sourceMysqlConfig", "source");
        MysqlConfig mysqlConfig = getMysqlConfig("sourceMysqlConfig", parameterTool);
        String host = mysqlConfig.getHost();
        Integer port = mysqlConfig.getPort();
        String userName = mysqlConfig.getUserName();
        String password = mysqlConfig.getPassword();

        Properties prop = new Properties();
        prop.setProperty("snapshot.locking.mode", "none");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(host)
                .port(port)
                .databaseList(database) // monitor all tables under inventory database
                .tableList(dbTable) // set captured table
                .username(userName)
                .password(password)
//                .serverTimeZone("UTC")  //时区
                .serverTimeZone("Asia/Shanghai")
                //设置读取位置 initial全量, latest增量,  specificOffset(binlog指定位置开始读,该功能新版本暂未支持)
                .startupOptions(StartupOptions.specificOffset(offsetFile, offsetPos))
//                .startupOptions(StartupOptions.initial())
//                .startupOptions(StartupOptions.latest())
                .debeziumProperties(prop)
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new DebeziumDeserializationSchema<String>() { //自定义数据解析器
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        //获取主题信息,包含着数据库和表名  mysql_binlog_source.test_db.test_table
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];

                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

                        //获取值信息并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        //获取变化后的数据
                        Struct after = value.getStruct("after");
                        Struct before = value.getStruct("before");

                        JSONObject after_data = new JSONObject();
                        JSONObject before_data = new JSONObject();

                        if (after != null) {
                            //创建JSON对象用于存储数据信息
                            for (Field field : after.schema().fields()) {
                                Object o = after.get(field);
                                after_data.put(field.name(), o);
                            }
                        }

                        if (before != null) {
                            //创建JSON对象用于存储数据信息
                            for (Field field : before.schema().fields()) {
                                Object o = before.get(field);
                                before_data.put(field.name(), o);
                            }
                        }

                        //创建JSON对象用于封装最终返回值数据信息
                        JSONObject result = new JSONObject();
                        result.put(OP, operation.toString().toUpperCase());
                        result.put(AFTER_DATA, after_data);
                        result.put(BEFORE_DATA, before_data);
                        result.put(DATABASE, db);
                        result.put(TABLE, tableName);

                        //发送数据至下游
                        collector.collect(result.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
//                        return null;
                        return TypeInformation.of(String.class);
                    }
                })
                .build();


        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        dataStreamSource.print("====>").setParallelism(1);


        List<String> opList = Arrays.asList(CREATE, DELETE, UPDATE);

        SingleOutputStreamOperator<BinlogRow> dataStream = dataStreamSource.filter((FilterFunction<String>) s -> {
            String op = JSONObject.parseObject(s).getString(OP);
            return opList.contains(op);
        }).keyBy((KeySelector<String, Long>) s -> {
            BinlogRow binlogRow = JSONObject.parseObject(s, BinlogRow.class);
            JSONObject afterData = binlogRow.getAfterData();
            if (afterData.containsKey(modField)) {
                return afterData.getLong(modField) % modNum;
            } else {
                JSONObject beforeData = binlogRow.getBeforeData();
                return beforeData.getLong(modField) % modNum;
            }
        }).map((MapFunction<String, BinlogRow>) s -> JSONObject.parseObject(s, BinlogRow.class));

        dataStream.addSink(new RichSinkFunction<BinlogRow>() {
            private ParameterTool parameterTool;
            private Connection mysqlConnection;
            private PreparedStatement pstmt;

            @Override
            public void open(Configuration parameters) throws Exception {
                parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                mysqlConnection = createMySQLConncetion();
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(BinlogRow value, Context context) throws Exception {
                String database = value.getDatabase();
                String table = value.getTable();

                String op = value.getOp();
                if (op.equals(CREATE) || op.equals(UPDATE)) {
                    JSONObject afterData = value.getAfterData();
                    long shard = afterData.getLong(modField) % modNum;
                    table += "_" + shard;

                    List<String> rowsList = new ArrayList<>();
                    List<String> valueList = new ArrayList<>();
                    List<String> placeholderList = new ArrayList<>();

                    for (Map.Entry<String, Object> kv : afterData.entrySet()) {
                        String k = kv.getKey();
                        String v = kv.getValue().toString();
                        rowsList.add(k);
                        valueList.add(v);
                        placeholderList.add("?");
                    }

                    String rows = String.join(",", rowsList);
                    String placeholders = String.join(",", placeholderList);

                    String sql = String.format("replace into %s.%s(%s) values (?,?,?)", database, table, rows, placeholders);
                    pstmt = mysqlConnection.prepareStatement(sql);

                    for (int i = 0; i < valueList.size(); i++) {
                        pstmt.setString(i + 1, valueList.get(i));
                    }
                    pstmt.addBatch();
                    int[] count = pstmt.executeBatch();
                    log.info("成功预执行了" + count.length + "行数据");
                    mysqlConnection.commit();

                } else {
                    // DELETE
                    JSONObject beforeData = value.getBeforeData();
                    long shard = beforeData.getLong(modField) % modNum;
                    table += "_" + shard;

                    Long pkValue = beforeData.getLong(primaryKey);
                    String sql = String.format("delete from %s.%s where %s=?", database, table, primaryKey);
                    pstmt = mysqlConnection.prepareStatement(sql);
                    pstmt.setLong(1, pkValue);

                    pstmt.addBatch();
                    int[] count = pstmt.executeBatch();
                    log.info("成功预执行了" + count.length + "行数据");
                    mysqlConnection.commit();
                }


            }

            public Connection createMySQLConncetion() throws SQLException {
                String sinkMysqlConfig = parameterTool.get("sinkMysqlConfig", "sink");
                MysqlConfig mysqlConifg = getMysqlConfig(sinkMysqlConfig, parameterTool);
                String host = mysqlConifg.getHost();
                String port = mysqlConifg.getPort().toString();
                String userName = mysqlConifg.getUserName();
                String password = mysqlConifg.getPassword();

                Connection mysqlConnection = JdbcUtil.createDruidConnectionPool(host, port, userName, password, database);
                mysqlConnection.setAutoCommit(false);
                return mysqlConnection;
            }
        });


        env.execute();
    }

    public static MysqlConfig getMysqlConfig(String configName, ParameterTool parameterTool) {
        String host = parameterTool.get(configName + "-mysql.host", "127.0.0.1");
        int port = parameterTool.getInt(configName + "-mysql.port", 3306);
        String userName = parameterTool.get(configName + "-mysql.username", "root");
        String password = parameterTool.get(configName + "-mysql.password", "root00");
        String dataBase = parameterTool.get(configName + "-mysql.database", "flink_test");

        return MysqlConfig.builder()
                .host(host)
                .port(port)
                .userName(userName)
                .password(password)
                .database(dataBase)
                .build();
    }

}


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
class BinlogRow {
    private String database;
    private String table;
    private JSONObject afterData;
    private JSONObject beforeData;
    private String op;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
class MysqlConfig {
    String host;
    Integer port;
    String userName;
    String password;
    String database;
}