package com.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 带超时的计数窗口触发器
 */
public class CountTriggerWithTimeout<T> extends Trigger<T, TimeWindow> {
    private static Logger LOG = LoggerFactory.getLogger(CountTriggerWithTimeout.class);
    /**
     * 窗口最大数据量
     */
    private int maxCount;
    /**
     * event time / process time
     */
    private TimeCharacteristic timeType;
    /**
     * 用于储存窗口当前数据量的状态对象
     */
    private ReducingStateDescriptor<Long> countStateDescriptor = new ReducingStateDescriptor("counter", new Sum(), LongSerializer.INSTANCE);

    public CountTriggerWithTimeout(int maxCount, TimeCharacteristic timeType) {
        this.maxCount = maxCount;
        this.timeType = timeType;
    }

    private TriggerResult fireAndPurge(TimeWindow window, TriggerContext ctx) throws Exception {
        clear(window, ctx);
        return TriggerResult.FIRE_AND_PURGE;
    }

    // 每个到达窗口的元素都要调用一次,判断是否需要结束窗口并往下游发送数据
    // element 到达的元素,timestamp到达元素的时间戳,window要添加元素的窗口,ctx可用于注册计时器回调的上下文对象
    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);

        countState.add(1L);
        if (countState.get() >= maxCount) {
            LOG.info("fire with count: " + countState.get());
            return fireAndPurge(window, ctx);
        }
        if (timestamp >= window.getEnd()) {
            LOG.info("fire with tiem: " + timestamp);
            return fireAndPurge(window, ctx);
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    // 当使用触发器上下文设置的处理时间计时器触发时调用
    // time计时器触发的时间戳,window定时器触发的窗口,ctx可用于注册计时器回调的上下文对象
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.ProcessingTime) {
            return TriggerResult.CONTINUE;
        }
        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            LOG.info("fire with process tiem: " + time);
            return fireAndPurge(window, ctx);
        }
    }

    // 当使用触发器上下文设置的事件时间计时器触发时调用
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.EventTime) {
            return TriggerResult.CONTINUE;
        }
        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            LOG.info("fire with event tiem: " + time);
            return fireAndPurge(window, ctx);
        }
    }

    // 清除触发器可能仍保持给定窗口的任何状态,当窗口被清除时调用
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState countState = ctx.getPartitionedState(countStateDescriptor);
        countState.clear();
    }

    /**
     * 计数方法
     */
    class Sum implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}