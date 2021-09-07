package com.sql.new_version.window;

import com.model.UserWatermark;
import com.model.WindowPlayerInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * https://helpcdn.aliyun.com/document_detail/62510.html?spm=a2c4g.11186623.6.822.24721476ef32ci
 * https://blog.csdn.net/qq_29342297/article/details/113774117
 */
public class SqlWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        /**
         * 新版本默认就是EventTime
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * 定义tableEnv
         */
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String filePath = "src/main/java/com/data/sql_window_data.txt";
        DataStream<String> inputDataStream = env.readTextFile(filePath);

        DataStream<WindowPlayerInfo> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            String club = fields[0];
            String userId = fields[1];
            String pos = fields[2];
            int age = Integer.parseInt(fields[3]);
            Long timestamp = Long.valueOf(fields[4]);
            return new WindowPlayerInfo(club, userId, pos, age, timestamp);
        });


        DataStream<WindowPlayerInfo> watermarkDataStream = dataStream.assignTimestampsAndWatermarks(
                /**
                 * 处理乱序数据
                 */
                new BoundedOutOfOrdernessTimestampExtractor<WindowPlayerInfo>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(WindowPlayerInfo windowPlayerInfo) {
                        return windowPlayerInfo.getTimestamp();
                    }
                });

        /**
         * 使用Flink sql
         */

        /**
         * rt.rowtime 表示处理时间EventTime
         * 其中rt 可以随便修改,但是后面的rowtime是规定的 不能随意修改,
         * rt.rowtime是一般约定俗成的写法
         *
         * 同理,如果是ProcessTime 一般写法是pt.proctime
         * pt可以修改,proctime不可以修改
         */
        Table table = tableEnv.fromDataStream(watermarkDataStream, "club,userId,pos,age,timestamp as ts,rt.rowtime");

        tableEnv.createTemporaryView("rt_info",table);

        /**
         * 滚动窗口
         * tumble_start 相当于 滚动窗口的WindowStart     返回窗口的起始时间（包含边界）如[00:10,00:15)窗口，返回00:10。
         * tumble_end 相当于 滚动窗口的WindowEnd         返回窗口的结束时间（包含边界）。例如[00:00, 00:15]窗口，返回00:15。
         * tumble_rowtime 相当于 滚动窗口的WindowEnd     返回窗口的结束时间（不包含边界）。例如[00:00, 00:15)窗口，返回00:14:59.999。
         * 只有基于 EventTime 的Window上可以使用,否则会报错
         *
         * tumble_proctime 相当于 滚动窗口的WindowEnd    返回窗口的结束时间（不包含边界）。例如[00:00, 00:15)窗口，返回00:14:59.999。
         * 只有基于 ProcessTime 的Window上可以使用, 否则 如果在EventTime的Window上使用显示为null
         */

        /**
         * 别名可以不指定
         * sql 最后不能加分号";"!
         * 10s的滚动窗口
         */
        Table tumbleSqlQuery = tableEnv.sqlQuery("select club," +
                                            "tumble_rowtime(rt, interval '10' seconds)," +
                                            "tumble_start(rt, interval '10' second)," +
                                            "tumble_end(rt, interval '10' second)," +
                                            "count(1) as cnt " +
                                            "from rt_info " +
                                            "group by club,tumble(rt,interval '10' second)");

        tableEnv.toAppendStream(tumbleSqlQuery, Row.class).print("tumbleWindow");

        /**
         * 滑动窗口 Table API
         */
        Table tumbleTableAPI = table.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("club,tw")
                .select("club,tw.rowtime,tw.start,tw.end,count(1)");
//        tableEnv.toAppendStream(tumbleTableAPI, Row.class).print("tumbleWindowTableAPI");


        /**
         * 滑动窗口
         * 滑动步长为2s,窗口大小为5s
         * 用法与滚动窗口一样,只需要把前缀 tumble改成hop
         *
         * hop_start
         * hop_end
         * hop_rowtime
         * hop_proctime
         */
        Table slideSqlQuery = tableEnv.sqlQuery("select " +
                "hop_rowtime(rt, interval '2' second,interval '5' second)," +
                "hop_start(rt, interval '2' second,interval '5' second)," +
                "hop_end(rt, interval '2' second,interval '5' second)," +
                "count(1) as cnt " +
                "from rt_info " +
                "group by hop(rt,interval '2' second,interval '5' second)");

//        tableEnv.toAppendStream(slideSqlQuery, Row.class).print("slideWindow");

        /**
         * 滑动窗口 Table API
         */
        Table slideTableAPI = table.window(Slide.over("5.seconds").every("2.seconds").on("rt").as("hw"))
                .groupBy("hw")
                .select("hw.rowtime,hw.start,hw.end,count(1)");
//        tableEnv.toAppendStream(slideTableAPI, Row.class).print("slideWindowTableAPI");

        /**
         * 会话窗口
         *
         * session_start
         * session_end
         * session_rowtime
         * session_proctime
         */
        Table sessionSqlQuery = tableEnv.sqlQuery("select " +
                "session_rowtime(rt, interval '2' second)," +
                "session_start(rt, interval '2' second)," +
                "session_end(rt, interval '2' second)," +
                "count(1) as cnt " +
                "from rt_info " +
                "group by session(rt,interval '2' second)");

//        tableEnv.toAppendStream(sessionSqlQuery, Row.class).print("sessionWindow");

        /**
         * 会话窗口 Table API
         */
        Table sessionTableAPI = table.window(Session.withGap("2.seconds").on("rt").as("sw"))
                .groupBy("sw")
                .select("sw.rowtime,sw.start,sw.end,count(1)");
//        tableEnv.toAppendStream(sessionTableAPI, Row.class).print("sessionWindowTableAPI");

        /**
         * OVER窗口
         */
        /**
         * 写法1
         */
        Table overSqlQuery = tableEnv.sqlQuery("select " +
                "*,count(1) " +
                "over (partition by club order by rt rows between 2 preceding and current row) " +
                "from rt_info");

//        tableEnv.toAppendStream(overSqlQuery, Row.class).print("overWindow");

        /**
         * 写法2
         */
        Table overSqlQuery2 = tableEnv.sqlQuery("select " +
                "club,rt," +
                "count(1) over w," +
                "sum(age) over w " +
                "from rt_info " +
                "window w as (partition by club order by rt rows between 1 preceding and current row)");

//        tableEnv.toAppendStream(overSqlQuery2, Row.class).print("overWindow2");

        /**
         * OVER窗口 TableAPI
         */
        Table overTableAPI = table.window(Over.partitionBy("club").orderBy("rt").preceding("1.rows").as("ow"))
                .select("club,rt,count(1) over ow,age.sum over ow");
//        tableEnv.toAppendStream(overTableAPI, Row.class).print("overWindow2TableAPI");


        env.execute();
    }
}
