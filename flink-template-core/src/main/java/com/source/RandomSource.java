package com.source;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomSource extends RichSourceFunction<Tuple2<String, String>> {
    private boolean isRunning = true;
    Random random = new Random();
    private final long sleepMilliSeconds;

    public RandomSource() {
        this.sleepMilliSeconds = 500;
    }

    public RandomSource(long sleepMilliSeconds) {
        this.sleepMilliSeconds = sleepMilliSeconds;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

        List<Tuple2<String, String>> randomInfo = new ArrayList<>();
        randomInfo.add(Tuple2.of("Elephant", "maybe"));
        randomInfo.add(Tuple2.of("Elephant", "fy"));
        randomInfo.add(Tuple2.of("Elephant", "eurus"));
        randomInfo.add(Tuple2.of("Elephant", "yang"));
        randomInfo.add(Tuple2.of("IG", "emo"));
        randomInfo.add(Tuple2.of("IG", "kaka"));
        randomInfo.add(Tuple2.of("LGD", "ame"));

        while (isRunning) {
            int index = random.nextInt(randomInfo.size());
            ctx.collect(randomInfo.get(index));
//            Thread.sleep(sleepMilliSeconds);
            TimeUnit.MILLISECONDS.sleep(sleepMilliSeconds);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
