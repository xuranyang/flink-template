package com.window;

import com.window.model.User;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

/**
 * Keyed Windows
 * <p>
 * stream
 * .keyBy(...)               // keyedStream上使用window
 * .window(...)              // 必选: 指定窗口分配器( window assigner)
 * [.trigger(...)]            // 可选: 指定触发器(trigger),如果不指定，则使用默认值
 * [.evictor(...)]            // 可选: 指定清除器(evictor),如果不指定，则没有
 * [.allowedLateness(...)]    // 可选: 指定是否延迟处理数据，如果不指定，默认使用0
 * [.sideOutputLateData(...)] // 可选: 配置side output，如果不指定，则没有
 * .reduce/aggregate/fold/apply/process() // 必选: 指定窗口计算函数
 * [.getSideOutput(...)]      // 可选: 从side output中获取数据
 */

/**
 * Non-Keyed Windows
 * <p>
 * stream
 * .windowAll(...)           // 必选: 指定窗口分配器( window assigner)
 * [.trigger(...)]            // 可选: 指定触发器(trigger),如果不指定，则使用默认值
 * [.evictor(...)]            // 可选: 指定清除器(evictor),如果不指定，则没有
 * [.allowedLateness(...)]    // 可选: 指定是否延迟处理数据，如果不指定，默认使用0
 * [.sideOutputLateData(...)] // 可选: 配置side output，如果不指定，则没有
 * .reduce/aggregate/fold/apply/process() // 必选: 指定窗口计算函数
 * [.getSideOutput(...)]      // 可选: 从side output中获取数据
 */
public class CommonWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long currentTimeMillis = System.currentTimeMillis();
        System.out.println("currentTimestamp:" + currentTimeMillis);

        DataStream<User> dataStream = env.fromElements(
                new User(1002L, "Maybe", 10, currentTimeMillis)
                , new User(1001L, "Ame", 40, currentTimeMillis + 1000L)
                , new User(1002L, "Maybe", 20, currentTimeMillis + 2000L)
                , new User(1001L, "Ame", 50, currentTimeMillis + 3000L)
                , new User(1002L, "Maybe", 30, currentTimeMillis + 4000L)
                , new User(1003L, "Chalice", 100, currentTimeMillis + 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<User>) (user, recordTimestamp) -> user.getTimestamp())
        );

        final OutputTag<User> outputTag = new OutputTag<User>("late-users") {
        };

        dataStream
                .keyBy((KeySelector<User, Long>) user -> user.getUserId() % 10)
//                .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(1)))                 // 滑动时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))                                   // 滚动时间窗口
                .trigger(PurgingTrigger.of(CountTrigger.of(1)))                                         // 触发器-窗口元素数量保留前N条
//                .evictor(CountEvictor.of(1))                                                          // 驱逐器-窗口元素数量保留后N条
                .evictor(new Evictor<User, TimeWindow>() {                                              // 驱逐器-自定义移除逻辑
                    /**
                     * Iterable elements：当前窗口中的元素
                     * int size：当前窗口中的元素数量
                     * W window：当前窗口
                     * EvictorContext evictorContext：evict的上下文
                     */
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<User>> iterable, int windowSize, TimeWindow timeWindow, EvictorContext evictorContext) {
                        // 移除窗口元素，在Window Function之前调用
                        System.out.println("【evictBefore】:" + windowSize);
                        System.out.println("[" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")");

                        Iterator<TimestampedValue<User>> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            TimestampedValue<User> nextUser = iterator.next();
                            User user = nextUser.getValue();
//                            long timestamp = nextUser.getTimestamp();
//                            System.out.println(user + "|" + timestamp);
                            // 移除Maybe
                            if ("Maybe".equals(user.getUserName())) {
                                iterator.remove();
                            }
                        }
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<User>> iterable, int windowSize, TimeWindow timeWindow, EvictorContext evictorContext) {
                        // 移除窗口元素，在Window Function之后调用
                        System.out.println("【evictAfter】:" + windowSize);
                        System.out.println("[" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")");
//                        for (TimestampedValue<User> userTimestampedValue : iterable) {
//                            User value = userTimestampedValue.getValue();
//                            long timestamp = userTimestampedValue.getTimestamp();
//                            System.out.println(value + "|" + timestamp);
//                        }
                    }
                })
                .allowedLateness(Time.seconds(0))                                                       // 指定允许数据延迟的时间
                .sideOutputLateData(outputTag)                                                          // 收集迟到的数据
                .process(new ProcessWindowFunction<User, String, Long, TimeWindow>() {                  // 指定窗口计算函数
                    @Override
                    public void process(Long key, Context context, Iterable<User> iterable, Collector<String> collector) throws Exception {
                        System.out.println("=========== KeyBy:" + key + " ===========");
                        // 通过context 获取 window
                        TimeWindow window = context.window();
                        long windowStart = window.getStart();
                        long windowEnd = window.getEnd();

                        System.out.println("[" + windowStart + "," + windowEnd + ")");
                        for (User user : iterable) {
                            System.out.println(user);
//                            collector.collect(user.toString());
                        }
                        System.out.println("=========== Window END ===========\n");
                    }
                })
                .getSideOutput(outputTag).print("outputTag >>")                                         // 处理侧输出的迟到数据
        ;

        env.execute();
    }
}
