package com.util.mock;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

public abstract class AbstractMockFlinkSource<T> extends RichSourceFunction<T> {
    private boolean isRun;
    private final long sleepMs;

    public AbstractMockFlinkSource() {
        this.sleepMs = 0L;
    }

    public AbstractMockFlinkSource(long sleepMs) {
        this.sleepMs = sleepMs;
    }

    // Flink Function
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRun = true;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRun) {
            mockData(ctx, sleepMs);
            TimeUnit.MILLISECONDS.sleep(sleepMs);
        }
    }

    @Override
    public void cancel() {
        isRun = false;
    }

    abstract void mockData(SourceContext<T> ctx, long sleepMs);

}
