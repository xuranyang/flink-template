package grv;

import com.model.UserInfo;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class FlinkGroovyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // nc -l -p 8888
        // 动态规则流：amount低于阈值为低价值用户,高于阈值为高价值用户
        // if(amount < 100){amt_type='low'}else{amt_type='high'}
        // if(amount < 200){amt_type='low'}else{amt_type='high'}
        DataStreamSource<String> ruleDs = env.socketTextStream("localhost", 8888);

        // nc -l -p 9999
        // 主数据流：数据为简单金额,如：150
        DataStreamSource<String> mainDs = env.socketTextStream("localhost", 9999);

        // 定义状态描述器
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("dynamicRules", Types.STRING, Types.STRING);
        BroadcastStream<String> broadcastDs = ruleDs.broadcast(descriptor);

        mainDs.connect(broadcastDs).process(new BroadcastProcessFunction<String, String, String>() {
            private Binding binding;
            private GroovyShell shell;
            private final String ruleName = "amountRule";
            private final String RULE_VAR_IN = "amount";
            private final String RULE_VAR_OUT = "amt_type";

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化 groovy 的 Binding 和 GroovyShell
                binding = new Binding();
                shell = new GroovyShell(binding);
            }

            @Override
            public void processElement(String s, ReadOnlyContext ctx, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                Integer amount = Integer.parseInt(s);
                String rule = broadcastState.get(ruleName);

                binding.setVariable(RULE_VAR_IN, amount); // 设置输入参数
                shell.evaluate(rule);   // GroovyShell进行计算
                String amtType = (String) binding.getVariable(RULE_VAR_OUT);    // 获取Groovy执行后的输出结果

                collector.collect(amtType);
            }

            @Override
            public void processBroadcastElement(String newValue, Context context, Collector<String> collector) throws Exception {
                BroadcastState<String, String> broadcastState = context.getBroadcastState(descriptor);
                System.out.println("旧规则:" + broadcastState.get(ruleName));
                // 清空旧数据,更新为新数据
                broadcastState.clear();
                broadcastState.put(ruleName, newValue);
                System.out.println("新规则:" + broadcastState.get(ruleName));
            }
        }).print();

        env.execute();
    }
}
