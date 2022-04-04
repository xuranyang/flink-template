package com.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * 【不使用greedy】:
 * a1,a2|a12
 * a1,a2,a12|b11
 * a1,a2|a12,b11
 * a2,a12|b11
 * a1,a2,a12|b11,b12
 * a2,a12|b11,b12
 * 其中分隔符|用来将匹配的第一个where和第二个where隔开，有结果可以看出a12这条记录会在两个where中都去匹配。
 * 1、读入a1，满足start的判断a开头，暂存[a1];
 * 2、读入a2，满足start的判断a开头，且上一个为[a1]，组合[a1,a2]满足整个start判断（2-3个a开头的），存入状态[a1,a2]；
 * 3、读入a12，满足start的判断a开头，存入状态[a1,a2,a12],[a2,a12]；同时也满足middle判断，存入状态[a12]，此时整个条件都满足，输出结果[a1,a2,|,a12];
 * 4、读入b11，满足middle判断，存入状态[a12,b11],[b11];此时整个条件都满足，输出[a1,a2,a12,|,b11],[a1,a2,|,a12,b11],[a2,a12,|,b11]
 * 5、读入b12，满足middle判断，存入状态[b11,b12],[b12];此时整个条件满足，输出[a1,a2,a12,|,b11,b12],[a2,a12,|,b11,b12]；由于[b12]是紧邻[b11]的，所以这里不会跳过[b11]而单独应用[b12],因此没有单独与[b12]匹配的结果；
 * 6、读入a3，满足start判断，但此时相当于又从头开始匹配了，存入状态[a3];
 *
 * 【使用greedy】:
 * a1,a2,a12|b11
 * a2,a12|b11
 * a1,a2,a12|b11,b12
 * a2,a12|b11,b12
 * 相较于不加greedy()，匹配结果变少了，a12只与前一个where匹配，忽略第二个where(即a12只会出现在|之前)
 */
public class GreedyDemo {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Event {
        private Long id;
        private String name;
        private String type;
        private Long timestamp;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //输入数据源
        DataStream<Event> input = env.fromCollection(Arrays.asList(
                new Event(1L, "a1", "add", 1588298400L),
                new Event(2L, "a2", "add", 1588298400L),
                new Event(3L, "a12", "add", 1588298400L),
                new Event(4L, "b11", "add", 1588298400L),
                new Event(5L, "b12", "add", 1588298400L),
                new Event(6L, "a3", "add", 1588298400L)
        )).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );

        //1、定义规则  匹配以a开头的用户
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().startsWith("a");
                    }
                }
        ).times(2, 3)
                .greedy() // 贪心,只能用在循环模式后,如连续出现了3次的话.times(2,3).greedy()只匹配连续3次的结果,不会匹配连续次的结果
                .next("middle").where(
                        new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event event) throws Exception {
                                return event.getName().length() == 3;
                            }
                        }).times(1, 2);

        //2、模式检测
        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        patternStream.select(
                new PatternSelectFunction<Event, String>() {
                    //返回匹配数据的id
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        return map.entrySet().stream()
                                .map(kv -> kv.getValue().stream().map(Event::getName).collect(Collectors.joining(",")))
                                .collect(Collectors.joining("|"));
                    }
                }
        ).print();

        env.execute("Greedy CEP Test");

    }

}
