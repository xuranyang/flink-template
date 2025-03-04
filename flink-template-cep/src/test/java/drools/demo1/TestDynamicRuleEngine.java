package drools.demo1;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

/*
============================ 数据流 ============================
{"userId": "abc", "age": 30, "level": "A"}
{"userId": "xyz", "age": 35, "level": "A"}
============================ 规则流 ============================
{"ruleId": "age_rule_v1", "version": 1, "drlContent": "package drools.demo1; import drools.demo1.Event; rule \"AgeRule\" when $event: Event(age > 25) then $event.setLevel(\"B\"); end"}
{"ruleId": "age_rule_v2", "version": 2, "drlContent": "package drools.demo1; import drools.demo1.Event; rule \"AgeRule\" when $event: Event(age > 25) then $event.setLevel(\"C\"); end"}

 */
public class TestDynamicRuleEngine {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // nc -l -p 8888
        // 1.数据流：模拟从 Kafka 消费业务数据
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 8888);
        DataStream<Event> dataDs = dataSource.map(s -> JSONObject.parseObject(s, Event.class));

        // nc -l -p 9999
        // 2. 规则流：模拟从 Kafka 消费规则更新事件
        DataStreamSource<String> ruleSource = env.socketTextStream("localhost", 9999);
        DataStream<Rule> ruleDs = ruleSource.map(s -> JSONObject.parseObject(s, Rule.class));

        // 3.1 定义状态描述器
        MapStateDescriptor<String, Rule> ruleDescriptor = new MapStateDescriptor<>("DynamicRules", TypeInformation.of(String.class), TypeInformation.of(Rule.class));
        // 3.2 将规则流广播到所有并行实例
        BroadcastStream<Rule> broadcastRuleDs = ruleDs.broadcast(ruleDescriptor);

        // 4. 连接数据流与广播规则流，使用 BroadcastProcessFunction 动态处理
        dataDs.connect(broadcastRuleDs).process(new BroadcastProcessFunction<Event, Rule, Event>() {
            // 使用单例模式管理 KieContainer（线程安全）
            private transient KieContainer kieContainer;
            // private final Object lock = new Object();
            private Object lock; // 更新锁

            @Override
            public void open(Configuration parameters) throws Exception {
                lock = new Object();
                // 初始化默认规则容器
                synchronized (lock) {
                    kieContainer = createKieContainer("");
                }
            }

            @Override
            public void processElement(Event event, ReadOnlyContext readOnlyContext, Collector<Event> out) throws Exception {
                synchronized (lock) {
                    StatelessKieSession session = kieContainer.newStatelessKieSession();
                    session.execute(event); // 执行规则
                    out.collect(event);
                }
            }

            @Override
            public void processBroadcastElement(Rule rule, Context context, Collector<Event> collector) throws Exception {
                synchronized (lock) {
                    try {
//                        BroadcastState<String, Rule> broadcastState = context.getBroadcastState(ruleDescriptor);
//                        broadcastState.put(rule.getRuleId(), rule);
//                        broadcastState.clear();
                        KieContainer newContainer = createKieContainer(rule.getDrlContent());
                        if (newContainer != null) {
                            kieContainer.dispose(); // 释放旧容器资源
                            kieContainer = newContainer;
                            System.out.println("规则更新成功:" + rule.getRuleId() + ", 版本:" + rule.getVersion());
                        }
                    } catch (Exception e) {
                        System.out.println("规则更新失败:" + rule.getRuleId());
                        e.printStackTrace();
                    }
                }
            }

            // 动态构建 KieContainer
            private KieContainer createKieContainer(String drlContent) {
                KieServices kieServices = KieServices.Factory.get();
                KieFileSystem kfs = kieServices.newKieFileSystem();
                kfs.write("src/main/resources/rules/dynamic_rule.drl", drlContent);

                KieBuilder kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
                if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
                    throw new RuntimeException("规则编译错误: " + kieBuilder.getResults().getMessages());
                }
                return kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());
            }
        }).name("Dynamic Rule Processor").print();

        env.execute();
    }

}

