package com.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce算子的合并操作至少需要两个元素,
 * 如 a,b,c都出现了2次,所以出发了合并操作
 * 如 d只出现了1次,不会触发合并操作
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(2);

        DataStreamSource<String> dataStream = env.fromElements("a,1", "b,1", "a,2", "c,3", "b,4", "d,5", "c,6");

        dataStream.map(new MapFunction<String, UserCount>() {
                    @Override
                    public UserCount map(String s) throws Exception {
                        String[] split = s.split(",");
                        String userId = split[0];
                        Integer count = Integer.parseInt(split[1]);
                        return new UserCount(userId, count);
                    }
                })
                .keyBy(UserCount::getUserId)
                .reduce(new ReduceFunction<UserCount>() {
                    @Override
                    public UserCount reduce(UserCount userCount1, UserCount userCount2) throws Exception {
                        UserCount userCount = new UserCount();
                        userCount.setUserId(userCount1.getUserId());
                        userCount.setCount(userCount1.getCount() + userCount2.getCount());

                        System.out.println(Thread.currentThread().getName() + "|" + userCount1 + "|" + userCount2);
                        return userCount;
                    }
                }).print();

        env.execute();
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class UserCount {
    String userId;
    Integer count;
}