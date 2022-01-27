package com.state;

import com.model.UserInfo;
import com.source.MySQLSource;
import com.source.RandomSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 可以参考
 * https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/broadcast_state/
 */
public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(BroadcastStateDemo.class);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> randomDataStream = env.addSource(new RandomSource());

        // 从mysql定时读取的小表数据
        DataStreamSource<Map<String, UserInfo>> mysqlDataStream = env.addSource(new MySQLSource());
//        randomDataStream.print();
        mysqlDataStream.print();

        // 定义状态描述器
        MapStateDescriptor<String, Tuple3<String, Integer, String>> descriptor = new MapStateDescriptor<>("info", Types.STRING, Types.TUPLE(Types.STRING, Types.INT, Types.STRING));

        // 配置广播流
        BroadcastStream<Map<String, UserInfo>> broadcast = mysqlDataStream.broadcast(descriptor);

        // 将事件流和广播流进行连接
        BroadcastConnectedStream<Tuple2<String, String>, Map<String, UserInfo>> connect = randomDataStream.connect(broadcast);

        SingleOutputStreamOperator<Tuple5<String, String, String, Integer, String>> result = connect.process(new BroadcastProcessFunction<Tuple2<String, String>, Map<String, UserInfo>, Tuple5<String, String, String, Integer, String>>() {
            @Override
            public void processElement(Tuple2<String, String> value,
                                       ReadOnlyContext ctx,
                                       Collector<Tuple5<String, String, String, Integer, String>> out) throws Exception {

                ReadOnlyBroadcastState<String, Tuple3<String, Integer, String>> broadcastState = ctx.getBroadcastState(descriptor);

                Tuple3<String, Integer, String> broadcastValue = broadcastState.get(value.f1);

                // 如果存在
                if (broadcastValue != null) {
                    String userName = broadcastValue.f0;
                    Integer age = broadcastValue.f1;
                    String nationality = broadcastValue.f2;
                    out.collect(Tuple5.of(value.f0, value.f1, userName, age, nationality));
                    log.info("[Success]");
                } else {
                    // 初始化第一次广播流没有数据，会走else
//                    [First Init]:(LGD,ame)
                    log.info("[First Init]:{}",value);
                    out.collect(Tuple5.of(value.f0, value.f1, null, null, null));
                }


            }


            //更新处理广播流中的数据
            @Override
            public void processBroadcastElement(Map<String, UserInfo> value,
                                                Context context,
                                                Collector<Tuple5<String, String, String, Integer, String>> out) throws Exception {
                BroadcastState<String, Tuple3<String, Integer, String>> broadcastState = context.getBroadcastState(descriptor);

                // 清空旧数据
                broadcastState.clear();

                for (String key : value.keySet()) {
                    UserInfo userInfo = value.get(key);
                    String userName = userInfo.getUserName();
                    Integer age = userInfo.getAge();
                    String nationality = userInfo.getNationality();
                    broadcastState.put(key, Tuple3.of(userName, age, nationality));
                }

            }
        });

        result.print();

        env.execute();
    }
}
