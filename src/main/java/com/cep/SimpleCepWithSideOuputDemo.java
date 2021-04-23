package com.cep;

import com.model.UserCep;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


/**
 * https://www.cnblogs.com/qiu-hua/p/13474481.html
 */
public class SimpleCepWithSideOuputDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputDataStream = env.readTextFile("src/main/java/com/data/cep_data2.txt");

        DataStream<UserCep> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            String userId = fields[0];
            Long timestamp = Long.valueOf(fields[1]);
            String status = fields[2];

            return new UserCep(userId, timestamp, status);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserCep>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(UserCep userCep) {
                return userCep.getTimestamp();
            }
        });

        KeyedStream<UserCep, String> keyedDataStream = dataStream.keyBy(UserCep::getUserId);

        /**
         * 定义侧输出流标签
         */
        OutputTag outputTag = new OutputTag<String>("TimeOutTag") {
        };

        /**
         * CEP
         * 5s内 每个userId 完成 begin->end
         * 定义一个匹配模式
         */
        Pattern<UserCep, UserCep> pattern = Pattern.<UserCep>begin("StepBegin").where(new IterativeCondition<UserCep>() {
            @Override
            public boolean filter(UserCep userCep, Context<UserCep> context) throws Exception {
                return userCep.getStatus().equals("begin");
            }
        })
                // 宽松近邻
                .followedBy("StepEnd").where(new IterativeCondition<UserCep>() {
                    @Override
                    public boolean filter(UserCep userCep, Context<UserCep> context) throws Exception {
                        return userCep.getStatus().equals("end");
                    }
                }).within(Time.seconds(5));

        /**
         * 将匹配模式应用到数据流DataStream上，得到PatternStream
         */
        PatternStream<UserCep> patternStream = CEP.pattern(keyedDataStream, pattern);


        /**
         * 没有侧输出流
         * 直接获取符合匹配条件的复杂事件，进行转换处理，得到处理结果
         */
//        patternStream.select(new EndSelectFunction()).print();

        /**
         * 使用侧输出流
         * 获取超时复杂事件
         */
        SingleOutputStreamOperator result = patternStream.select(outputTag, new TimeoutSelectFunction(), new EndSelectFunction());

        result.print("EndResult");
        result.getSideOutput(outputTag).print("TimeoutResult");

        env.execute();
    }


    /**
     * 自定义超时事件处理函数
     */
    public static class TimeoutSelectFunction implements PatternTimeoutFunction<UserCep, String> {

        @Override
        public String timeout(Map<String, List<UserCep>> map, long timeoutTimestamp) throws Exception {
            String userId = map.get("StepBegin").iterator().next().getUserId();
            return userId + " is Not End,Timeout Timestamp:" + timeoutTimestamp;
        }
    }


    /**
     * 自定义正常匹配事件处理函数
     */
    public static class EndSelectFunction implements PatternSelectFunction<UserCep, String> {

        @Override
        public String select(Map<String, List<UserCep>> map) throws Exception {
            String userId = map.get("StepEnd").iterator().next().getUserId();
            return userId + " is End";
        }
    }

}
