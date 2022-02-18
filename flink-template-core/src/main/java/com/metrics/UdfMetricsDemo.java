package com.metrics;

import com.source.RandomSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

public class UdfMetricsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(new RandomSource(200));
        SingleOutputStreamOperator<String> dataStream = dataStreamSource.map(element -> element.f0 + "_" + element.f1);
//        SingleOutputStreamOperator<Long> longDataStream = dataStreamSource.map(element -> 1L);
        SingleOutputStreamOperator<Long> longDataStream = dataStreamSource.map(
                element -> 1L + (long) new Random().nextInt(10)
        );

        dataStream.map(new CounterMapper()).print("Counter");
        dataStream.map(new GaugeMapper()).print("Gauge");
        longDataStream.map(new HistogramMapper()).print("Histogram");
        longDataStream.map(new MeterMapper()).print("Meter");

        env.execute();
    }
}
