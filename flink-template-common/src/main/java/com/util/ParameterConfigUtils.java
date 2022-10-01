package com.util;

import lombok.Data;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


@Data
public class ParameterConfigUtils {
    enum FlinkEnvConfigEnum {
        Checkpoint("chk"),
        Parallelism("parallelism"),
        RuntimeMode("runtimeMode");

        private String configName;

        FlinkEnvConfigEnum(String configName) {
            this.configName = configName;
        }

        public String getConfigName() {
            return configName;
        }

        public void setConfigName(String configName) {
            this.configName = configName;
        }
    }

    private StreamExecutionEnvironment env;
    private ParameterTool parameterTool;

//    public ParameterConfigUtils(StreamExecutionEnvironment env, ParameterTool parameterTool) {
//        this.env = env;
//        this.parameterTool = parameterTool;
//    }

    public static void setCheckpoint(StreamExecutionEnvironment env, ParameterTool parameterTool, String configName) {
        long chk = parameterTool.getLong(configName, -1L);
        if (chk > 0) {
            env.enableCheckpointing(chk);
        }
    }

    public static void setParallelism(StreamExecutionEnvironment env, ParameterTool parameterTool, String configName) {
        int parallelism = parameterTool.getInt(configName, -1);
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
    }

    public static void setRuntimeMode(StreamExecutionEnvironment env, ParameterTool parameterTool, String configName) {
        String runtimeMode = parameterTool.get(configName, null);
        if ("streaming".equalsIgnoreCase(runtimeMode)) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        } else if ("batch".equalsIgnoreCase(runtimeMode)) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
    }

    public static void setFlinkParameterConfig(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        setCheckpoint(env, parameterTool, FlinkEnvConfigEnum.Checkpoint.configName);
        setParallelism(env, parameterTool, FlinkEnvConfigEnum.Parallelism.configName);
        setRuntimeMode(env, parameterTool, FlinkEnvConfigEnum.RuntimeMode.configName);
    }
}
