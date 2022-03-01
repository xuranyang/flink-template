package com.state;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AutoStateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(2);

        DataStreamSource<Tuple2<String, String>> source = getSource(env);


        source.keyBy(e -> e.f0).process(new KeyedProcessFunction<String, Tuple2<String, String>, String>() {
//            MapState<String, List<String>> mapState;
            MapState<String, String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 设置状态过期配置
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(3))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
//                MapStateDescriptor<String, List<String>> mapStateDescriptor = new MapStateDescriptor<>("ttl-map-state", Types.STRING, Types.LIST(TypeInformation.of(String.class)));
//                MapStateDescriptor<String, List<String>> mapStateDescriptor = new MapStateDescriptor<>("ttl-map-state", Types.STRING, Types.LIST(TypeInformation.of(new TypeHint<String>() {})));

                MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("ttl-map-state", Types.STRING, Types.STRING);
                // 状态过期配置与壮状态绑定
                mapStateDescriptor.enableTimeToLive(ttlConfig);
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, String> value, Context context, Collector<String> collector) throws Exception {
                mapState.put(value.f1, value.f0);

                System.out.println("[TTL MapState " + context.getCurrentKey() + "]");

                Iterator<Map.Entry<String, String>> iterator = mapState.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> next = iterator.next();
                    System.out.println(context.getCurrentKey() + "->" + next.getKey() + ":" + next.getValue());
                }

                System.out.println("====End TTL MapState " + context.getCurrentKey() + "====");
                collector.collect("[CurrentTime]:" + LocalDateTime.now());
            }
        }).print();


        env.execute();
    }

    public static DataStreamSource<Tuple2<String, String>> getSource(StreamExecutionEnvironment env) {
        DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(new SourceFunction<Tuple2<String, String>>() {
            boolean tag = true;
            List<String> keyList = Lists.newArrayList("a", "b", "c");

            @Override
            public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
                while (tag) {
                    for (String key : keyList) {
                        sourceContext.collect(Tuple2.of(key, LocalDateTime.now().toString()));
                    }
                    TimeUnit.MILLISECONDS.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                tag = false;
            }
        });

        return dataStreamSource;
    }
}
