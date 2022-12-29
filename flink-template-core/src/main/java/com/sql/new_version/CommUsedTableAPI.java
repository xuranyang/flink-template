package com.sql.new_version;

import com.model.PlayerInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.descriptors.Csv;
//import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * https://blog.csdn.net/wangzhongyudie/article/details/124732171
 */
public class CommUsedTableAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String filePath = "flink-template-core/src/main/java/com/data/data2.txt";

        /**
         * 常用Table API
         */

        /**
         * 新版Flink1.14的createTemporaryTable用法
         * 直接读取指定Source数据注册成Table(不通过DataStream间接转换成Table)
         */
        tableEnv.createTemporaryTable("t_info", TableDescriptor.forConnector("filesystem")
                .option("path", filePath)
                .format(FormatDescriptor.forFormat("csv").option("field-delimiter", ",").build())
                .schema(org.apache.flink.table.api.Schema.newBuilder()
                        .column("club", DataTypes.STRING())
                        .column("userId", DataTypes.STRING())
                        .column("pos", DataTypes.TINYINT())
                        .column("age", DataTypes.INT())
                        .build())
                .build());
        /*
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("club", DataTypes.STRING())
                        .field("userId", DataTypes.STRING())
                        .field("pos", DataTypes.TINYINT())
                        .field("age", DataTypes.INT())
                )
                .createTemporaryTable("t_info");
        */

        Table t_info = tableEnv.from("t_info");
        Table filterTable = t_info.select("club,userId,pos,age").filter("club=='EHOME'");
        Table whereTable = t_info.select("club,userId,pos,age").where("club=='Elephant'");
        Table distinctTable = t_info.select("club").distinct();
        /**
         * 聚合函数以下这两种写法都可以
         */
        Table groupByTable = t_info.groupBy("club").select("club,club.count,pos.min,age.sum,age.avg");
        Table groupByTable2 = t_info.groupBy("club").select("club,count(club),min(pos),sum(age),avg(age)");

        /**
         * Table API -> DataStream
         */
        tableEnv.toAppendStream(filterTable, Row.class).print("filterAPI");
        tableEnv.toAppendStream(whereTable, Row.class).print("whereAPI");
        tableEnv.toRetractStream(distinctTable, Row.class).print("distinctAPI");

        tableEnv.toRetractStream(groupByTable, Row.class).print("groupByAPI");
        tableEnv.toRetractStream(groupByTable2, Row.class).print("groupBy2API");

        env.execute();
    }
}
