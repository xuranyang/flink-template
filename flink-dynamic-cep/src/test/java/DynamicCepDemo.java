import cep.CEP;
import cep.PatternStream;
import cep.functions.DynamicPatternFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
1001,1656914303000,success
1001,1656914304000,success
1001,1656914305000,fail
1001,1656914306000,success
1001,1656914307000,fail
1001,1656914308000,success
1001,1656914309000,fail
1001,1656914310000,success
1001,1656914311000,fail
1001,1656914312000,fail
1001,1656914313000,success
1001,1656914314000,success
1001,1656914315000,success
1001,1656914316000,fail
1001,1656914317000,fail
1001,1656914318000,fail
1001,1656914319000,fail
1001,1656914317000,end
*/
public class DynamicCepDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // nc -lp 8888
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, Long, String>> source = dataStreamSource.map(new MapFunction<String, Tuple3<String, Long, String>>() {
                    @Override
                    public Tuple3<String, Long, String> map(String s) throws Exception {
                        System.out.println(s);
                        String[] split = s.split(",");
                        return Tuple3.of(split[0], Long.valueOf(split[1]), split[2].replace(" ", ""));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> {
                            return event.f1;
                        }));

        PatternStream<Tuple3<String, Long, String>> patternStream = CEP.injectionPattern(source, new TestDynamicPatternFunction());

        patternStream.select(new PatternSelectFunction<Tuple3<String, Long, String>, Map>() {
            @Override
            public Map select(Map<String, List<Tuple3<String, Long, String>>> pattern) throws Exception {
                System.out.println("pattern->" + pattern);
                return pattern;
            }
        }).print();
        env.execute("DynamicCep");

    }

    public static class TestDynamicPatternFunction implements DynamicPatternFunction<Tuple3<String, Long, String>> {

        public TestDynamicPatternFunction() {
            this.flag = true;
        }

        boolean flag;
        int time = 0;

        @Override
        public void init() throws Exception {
            flag = true;
        }

        @Override
        public Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> inject()
                throws Exception {

            // 随机2种pattern
            // 1.success 必须连续匹配2次，接下来是fail
            // 2.success 连续匹配3次，接下来fail
            if (flag) {
                return Pattern
                        .<Tuple3<String, Long, String>>begin("start")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                                  Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("success");
                            }
                        })
                        .times(2)
                        .followedBy("middle")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                                  Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("fail");
                            }
                        })
                        .times(1)
                        .next("end");
            } else {
                return Pattern
                        .<Tuple3<String, Long, String>>begin("start2")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                                  Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("success");
                            }
                        })
                        .times(3)
                        .followedBy("middle2")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                                  Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("fail");
                            }
                        })
                        .times(1)
                        .next("end2");
            }
        }

        @Override
        public long getPeriod() throws Exception {
            System.out.println("Period");
            return 30000;
        }

        @Override
        public boolean isChanged() throws Exception {
            flag = !flag;
            time += getPeriod();
            System.out.println("change pattern : " + time);
            return true;
        }
    }
}
