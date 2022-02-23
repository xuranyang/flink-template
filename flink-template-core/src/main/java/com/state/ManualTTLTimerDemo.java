package com.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 手动过期
 * 只保留最近 指定 时间内的元素,对其属性求和
 */
public class ManualTTLTimerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<MyElement> dataStreamSource = getSource(env);
//        dataStreamSource.print();

        DataStream<MyElement> dataStream = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<MyElement>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<MyElement>() {
                    @Override
                    public long extractTimestamp(MyElement myElement, long l) {
                        return myElement.getTs();
                    }
                }));

        dataStream.keyBy(MyElement::getName).process(new onTimeCleanElementProcess()).print();

        env.execute();
    }

    public static DataStreamSource<MyElement> getSource(StreamExecutionEnvironment env) {
        DataStreamSource<MyElement> dataStreamSource = env.addSource(new SourceFunction<MyElement>() {
            // List<String> nameList = Arrays.asList("a", "b", "c");
            List<String> nameList = Arrays.asList("a");
            Integer num = 0;
            boolean tag = true;

            @Override
            public void run(SourceContext<MyElement> ctx) throws Exception {
                while (tag) {
                    num += 1;
//
                    MyElement myElement = new MyElement();
                    myElement.setName(nameList.get(new Random().nextInt(nameList.size())));
                    myElement.setNum(num);
                    myElement.setTs(new Date().getTime());

                    TimeUnit.MILLISECONDS.sleep(1000);
                    ctx.collect(myElement);
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

@Data
@AllArgsConstructor
@NoArgsConstructor
class MyElement {
    private String name;
    private Integer num;
    private Long ts;
}

class onTimeCleanElementProcess extends KeyedProcessFunction<String, MyElement, String> {
    public static int TTL = 2;
    MapState<String, List<MyElement>> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("keyd-map-state", Types.STRING, Types.LIST(TypeInformation.of(MyElement.class))));
    }

    @Override
    public void close() throws Exception {
        if (mapState != null) {
            mapState.clear();
        }
    }

    @Override
    public void processElement(MyElement value, Context ctx, Collector<String> out) throws Exception {
        // 获取EventTime
        // ctx.timestamp() 等于 value.getTs()
        Long eventTimestamp = ctx.timestamp();

        // 按name KeyBy
        String name = ctx.getCurrentKey();

        // 获取ProcessTime
        long procTime = ctx.timerService().currentProcessingTime();
//                System.out.printf("[timestamp]:%s,[ProcessingTime]:%s,[Value]:%s\n", eventTimestamp, procTime, value);

        // 根据eventTime注册一个延迟 TTL 秒的定时器
        ctx.timerService().registerEventTimeTimer(eventTimestamp + TTL * 1000L);


//      不包含name说明是第一次需要初始化
        if (mapState.contains(name)) {
            List<MyElement> elementList = mapState.get(name);
            elementList.add(value);
            mapState.put(name, elementList);
        } else {
            List<MyElement> initElementList = new ArrayList<>();
            initElementList.add(value);
            mapState.put(name, initElementList);
        }

        System.out.println("[MapState]->" + mapState.entries());

        for (String key : mapState.keys()) {
            List<MyElement> myElements = mapState.get(key);

            // 对Lists中每个MyElement的num求和
            Integer sum = myElements.stream().map(MyElement::getNum).reduce(0, Integer::sum);
            out.collect("[OutputSum]" + key + ":" + sum);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 清理已经触发的定时器
//        ctx.timerService().deleteEventTimeTimer(timestamp);

        // 触发定时器的时间 timestamp == ctx.timestamp()
        String name = ctx.getCurrentKey();

        System.out.printf("[Delete]->Key:%s,定时器触发时间:%s\n", name, timestamp);
        System.out.println("[DeleteBefore]" + mapState.entries());

        // 延迟了 TTL 秒
        long eventTimestamp = timestamp - TTL * 1000L;
        System.out.println("触发定时器删除指定时间的元素:" + eventTimestamp);

        List<MyElement> elementList = mapState.get(name);
        // 手动清理要过期删除的元素, 过期元素的ts 等于 eventTimestamp
        // 即只保留 line.getTs() > eventTimestamp
        List<MyElement> cleanList = elementList.stream().filter(line -> eventTimestamp != line.getTs()).collect(Collectors.toList());

        mapState.put(name, cleanList);
        System.out.println("[DeleteAfter]" + mapState.entries());

    }

}
