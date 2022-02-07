package com.sql;

import com.udf.SplitUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class UdfDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
//        TableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String filePath = "flink-template-sql/src/main/java/com/data/split_str.txt";

        tableEnv.createTemporarySystemFunction("split_udtf", SplitUDTF.class);

        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("club", DataTypes.STRING())
                        .field("players", DataTypes.STRING())
                )
                .createTemporaryTable("t_users");

        String sql="select club,player from t_users,LATERAL TABLE(split_udtf(players,'\\|')) AS T(player)";
        Table resultTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        tableEnv.executeSql(sql).print();

        env.execute();
    }
}
