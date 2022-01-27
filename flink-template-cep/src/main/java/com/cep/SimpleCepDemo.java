package com.cep;

import com.model.UserCep;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;


/**
 * https://www.cnblogs.com/qiu-hua/p/13474481.html
 */
public class SimpleCepDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputDataStream = env.readTextFile("flink-template-cep/src/main/java/com/data/cep_data.txt");

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
         * CEP
         * 1s内 分区连续出现 success -> fail -> success
         * 定义一个匹配模式
         */
        Pattern<UserCep, UserCep> pattern = Pattern.<UserCep>begin("1stSuccess").where(new SimpleCondition<UserCep>() {
            @Override
            public boolean filter(UserCep userCep) throws Exception {
                return userCep.getStatus().equals("success");
            }
        }).next("2ndFail").where(new SimpleCondition<UserCep>() {
            @Override
            public boolean filter(UserCep userCep) throws Exception {
                return userCep.getStatus().equals("fail");
            }
        }).next("3rdSuccess").where(new SimpleCondition<UserCep>() {
            @Override
            public boolean filter(UserCep userCep) throws Exception {
                return userCep.getStatus().equals("success");
            }
        }).within(Time.seconds(1));


        /**
         * 将匹配模式应用到数据流DataStream上，得到PatternStream
         */
        PatternStream<UserCep> patternStream = CEP.pattern(keyedDataStream, pattern);


        /**
         * 获取符合匹配条件的复杂事件，进行转换处理，得到处理结果
         */
        patternStream.select(new PatternSelectFunction<UserCep, String>() {
            @Override
            public String select(Map<String, List<UserCep>> map) throws Exception {
                UserCep firstSuccess = map.get("1stSuccess").get(0);
                UserCep secondFail = map.get("2ndFail").get(0);
                UserCep thirdSuccess = map.get("3rdSuccess").get(0);

                return firstSuccess.toString() + "|" + secondFail.toString() + "|" + thirdSuccess.toString();
            }
        }).print();


        env.execute();
    }
}
