package com.cep;

import com.model.UserCep;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * https://www.cnblogs.com/smile-xiaoyong/p/12867302.html
 */
public class SimpleCepDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputDataStream = env.readTextFile("flink-template-cep/src/main/java/com/data/cep_data.txt");

        DataStream<UserCep> dataStream = env.fromElements(
                new UserCep("ame", 1618842801000L, "success"),
                new UserCep("ame", 1618842801001L, "fail"),
                new UserCep("ame", 1618842801001L, "success"),
                new UserCep("maybe", 1618842801002L, "fail"),
                new UserCep("maybe", 1618842801003L, "fail"),
                new UserCep("maybe", 1618842801004L, "fail"),
                new UserCep("maybe", 1618842801005L, "success"),
                new UserCep("maybe", 1618842801006L, "fail")
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<UserCep>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<UserCep>() {
                    @Override
                    public long extractTimestamp(UserCep element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );

        KeyedStream<UserCep, String> keyedDataStream = dataStream.keyBy(UserCep::getUserId);

        /**
         * CEP
         * 定义一个匹配模式
         */
        Pattern<UserCep, UserCep> pattern = Pattern.<UserCep>begin("first")
                .where(new SimpleCondition<UserCep>() {
                    @Override
                    public boolean filter(UserCep userCep) throws Exception {
                        return userCep.getStatus().equals("fail");
                    }
                })
//                .followedBy("second") // 宽松近邻
                .oneOrMore() // 出现1次或多次
//                .timesOrMore(2) // 出现2次或多次
//                .times(3) // 出现3次
//                .times(2, 4) // 2~4次
//                .optional() // 可以满足,也可以不满足
//                .greedy() // 贪心,只能用在循环模式后,如连续出现了4次的话.times(2,4).greedy()只匹配连续4次的结果;greedy()只有在多个pattern中使用时才起作用。在单个Pattern中使用时与不加greedy()是一样的
                .consecutive()// 指定为严格近邻,默认是宽松近邻
//                .allowCombinations()  // 不确定连续
//                .within(Time.seconds(1)) //时间限制,匹配在1s内发生
                ;


        /**
         * 将匹配模式应用到数据流DataStream上，得到PatternStream
         */
        PatternStream<UserCep> patternStream = CEP.pattern(keyedDataStream, pattern);


        /**
         * 获取符合匹配条件的复杂事件，进行转换处理，得到处理结果
         */
//        patternStream.select(new PatternSelectFunction<UserCep, String>() {
//            @Override
//            public String select(Map<String, List<UserCep>> map) throws Exception {
//                return map.toString();
//            }
//        }).print();

        patternStream.process(new PatternProcessFunction<UserCep, String>() {
            @Override
            public void processMatch(Map<String, List<UserCep>> map, Context context, Collector<String> collector) throws Exception {
                collector.collect(map.toString());
            }
        }).print();


        env.execute();
    }
}
