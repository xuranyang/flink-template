package com.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.TimeZone;

/*
1000000009000,a
1000000010000,b
1000000011000,a
1000000012000,c
1000000019999,d
1000000020000,e
 */
public class EventTimeWindowDemo {
    private static final ThreadLocal<DateFormat> dateTimeMsDf = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    public static String timestampToDatetimeMs(Long timestamp, String timeZone) {
        DateFormat dateFormat = dateTimeMsDf.get();
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        timestamp = timestamp.toString().length() == 10 ? timestamp * 1000 : timestamp;
        return dateFormat.format(new Date(timestamp));
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // nc -lp1234
        final DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 1234);

        // EventTime, Watermark -> 0s
        final SingleOutputStreamOperator<Element> dataStream = dataStreamSource.map((MapFunction<String, Element>) s -> {
            final String[] split = s.split(",");
            return new Element(split[1], Long.valueOf(split[0]));
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Element>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Element>) (e, l) -> e.getTimestamp())
        );

        dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .trigger(new CountTriggerWithTimeout(2, TimeCharacteristic.EventTime))
//                .allowedLateness(Time.seconds(0))
                .process(new ProcessAllWindowFunction<Element, String, TimeWindow>() {
                    private static final String timezone = "GMT+08:00";

                    @Override
                    public void process(Context context, Iterable<Element> iterable, Collector<String> collector) throws Exception {
                        final TimeWindow window = context.window();
                        final long windowStartTs = window.getStart();
                        final long windowEndTs = window.getEnd();

                        final String windowStartTime = timestampToDatetimeMs(windowStartTs, timezone).split(" ")[1];
                        final String windowEndTime = timestampToDatetimeMs(windowEndTs, timezone).split(" ")[1];

                        for (Element element : iterable) {
                            final String eventTime = timestampToDatetimeMs(element.getTimestamp(), timezone).split(" ")[1];
                            // [windowStart,windowEnd) 左闭右开
                            String info = "[" + windowStartTime + "," + windowEndTime + ")[" + eventTime + "]" + element;
                            collector.collect(info);
                        }
                    }
                })
                .print();

        env.execute();
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class Element {
    private String id;
    private Long timestamp;
}