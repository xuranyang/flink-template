package com.nfa;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class NFADemo {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class LogEvent {
        private String name;
        private String eventType;
        private Long timestamp;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<LogEvent, String> dataStream = env.fromElements(
                new LogEvent("ame", "fail", 1000L),
                new LogEvent("ame", "fail", 2000L),
                new LogEvent("maybe", "fail", 3000L),
                new LogEvent("ame", "fail", 4000L),
                new LogEvent("maybe", "success", 5000L),
                new LogEvent("maybe", "fail", 6000L),
                new LogEvent("maybe", "fail", 7000L)
        ).keyBy(e -> e.name);

        // 数据按照顺序依次输入,用状态机进行处理,状态跳转
        dataStream.flatMap(new StateMachineMapper()).print();

        env.execute();
    }

    public static class StateMachineMapper extends RichFlatMapFunction<LogEvent, String> {
        ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LogEvent value, Collector<String> out) throws Exception {
            // 如果状态为空,进行初始化
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            // 跳转到下一状态
            State nextState = state.transition(value.eventType);

            // 判断当前状态的特殊情况,直接进行跳转
            if (nextState == State.Matched) {
                // 检测到了匹配,输出报警信息;不更新状态就是跳转回S2
                out.collect(value.name + "连续3次登录失败");
            } else if (nextState == State.Terminal) {
                //直接将状态更新为初始状态,重新开始检测
                currentState.update(State.Initial);
            } else {
                currentState.update(nextState);
            }

        }


    }

    // 状态机实现
    public enum State {
        Terminal,   //匹配失败,终止状态
        Matched,    //匹配成功

        // S2状态,传入基于S2状态可以进行的一系列状态转移
        S2(new Transition("fail", Matched), new Transition("success", Terminal)),

        // S1状态
        S1(new Transition("fail", S2), new Transition("success", Terminal)),

        // 初始状态
        Initial(new Transition("fail", S1), new Transition("success", Terminal));

        private Transition[] transitions;   //当前状态的转移规则

        State(Transition... transitions) {
            this.transitions = transitions;
        }

        //状态转移方法
        public State transition(String eventType) {
            for (Transition transition : transitions) {
                if (transition.getEventType().equals(eventType)) {
                    return transition.getTargetState();
                }
            }

            // 回到初始状态
            return Initial;
        }

    }

    // 定义一个状态转移类,包含当前引起状态转移的事件类型,以及转移的目标状态
    @Data
    public static class Transition {
        private String eventType;
        private State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }

    }

}
