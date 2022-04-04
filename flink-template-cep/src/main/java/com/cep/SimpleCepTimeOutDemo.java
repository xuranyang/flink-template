package com.cep;

import com.model.UserCep;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * https://www.cnblogs.com/smile-xiaoyong/p/12867302.html
 */
public class SimpleCepTimeOutDemo {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class OrderEvent {
        private String name;
        private String type;
        private Long timestamp;
    }

//    private static OutputTag<String> timeoutTag;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputDataStream = env.readTextFile("flink-template-cep/src/main/java/com/data/cep_data.txt");

        DataStream<OrderEvent> dataStream = env.fromElements(
                new OrderEvent("ame", "create", 1000L),
                new OrderEvent("ame", "create", 2000L),
                new OrderEvent("ame", "modify", 3000L),
                new OrderEvent("maybe", "create", 4000L),
                new OrderEvent("maybe", "modify", 5000L),
                new OrderEvent("maybe", "pay", 6000L),
                new OrderEvent("maybe", "create", 7000L),
                new OrderEvent("maybe", "pay", 8000L),
                new OrderEvent("fy", "pay", 2000L),
                new OrderEvent("fy", "create", 1000L),
                new OrderEvent("fy", "create", 100000L),
                new OrderEvent("fy", "modify", 1500L), //迟到数据
                new OrderEvent("fy", "create", 200000L),
                new OrderEvent("fy", "modify", 1500L),

                new OrderEvent("chalice", "create", 2000L),
                new OrderEvent("chalice", "pay", 1000L),
                new OrderEvent("ana", "create", 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );

        KeyedStream<OrderEvent, String> keyedDataStream = dataStream.keyBy(OrderEvent::getName);

        /**
         * CEP
         * 定义一个匹配模式
         */
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getType().equals("create");
                    }
                }).followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getType().equals("pay");
                    }
                })
                .within(Time.seconds(5));


        /**
         * 将匹配模式应用到数据流DataStream上，得到PatternStream
         */
        PatternStream<OrderEvent> patternDataStream = CEP.pattern(keyedDataStream, pattern);

        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {};
        OutputTag<OrderEvent> lateDataTag = new OutputTag<OrderEvent>("late-data") {};


        /**
         * 获取符合匹配条件的复杂事件，进行转换处理，得到处理结果
         */
        SingleOutputStreamOperator<String> result = patternDataStream
                .sideOutputLateData(lateDataTag)
                .process(new OrderPayProcess());

        result.print();
        result.getSideOutput(timeoutTag).print("TimeOut>>");
        result.getSideOutput(lateDataTag).print("LateData>>");

        env.execute();
    }

    public static class OrderPayProcess extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler {

        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {
            collector.collect("[Payed]:" + map.toString());
        }

        @Override
        public void processTimedOutMatch(Map map, Context context) throws Exception {
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
            };
            context.output(timeoutTag, "[TimeOut]:" + map.toString());
        }
    }
}
