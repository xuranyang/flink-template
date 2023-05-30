package com.util;

import com.constant.PropertiesConstants;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Maps;
import com.model.FlinkProperties;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

public class FlinkUtils {
    public static StreamExecutionEnvironment createEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static StreamExecutionEnvironment createEnv(long chkInterval) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(chkInterval);
        return env;
    }

    public static StreamExecutionEnvironment initStreamExecEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(60000L);

        return env;
    }

    public static ParameterTool createParameterTool() throws Exception {
        try {
            return ParameterTool
                    .fromPropertiesFile(FlinkUtils.class.getResourceAsStream(PropertiesConstants.GLOBAL_PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool
                .fromPropertiesFile(FlinkUtils.class.getResourceAsStream(PropertiesConstants.GLOBAL_PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());
    }


    public static ParameterTool getParameterTool(final String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // 根据 env_type 的类型选择对应的配置文件
        String propType = parameterTool.get(PropertiesConstants.ENV_TYPE);
        String propertiesFileName = CommonUtils.getGlobalPropertiesFileName(propType);

        return parameterTool
                .mergeWith(ParameterTool.fromPropertiesFile(FlinkUtils.class.getResourceAsStream(propertiesFileName)))
                .mergeWith(ParameterTool.fromSystemProperties());
    }


    // -------------------------------------- FlinkProperties -----------------------------------------

    /**
     * 自定义封装类，从Map中抽取指定Key到指定Model中
     *
     * @return
     */
    public static <T> T convertProperties(T model, Map<String, String> parametersMap) throws Exception {
        Map<String, String> resultMap = Maps.newHashMap();

//        Field[] declaredFields = FlinkProperties.class.getDeclaredFields();
        Field[] declaredFields = model.getClass().getDeclaredFields();
        for (Field declaredField : declaredFields) {
            String name = declaredField.getName();
            // 驼峰转换: testData -> test_data
            String toCamelName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
            // 驼峰转换: test_data -> testData
//            String toCamelName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);

            if (parametersMap.containsKey(name)) {
                resultMap.put(name, parametersMap.get(name));
            } else if (!parametersMap.containsKey(name) && parametersMap.containsKey(toCamelName)) {
                resultMap.put(name, parametersMap.get(toCamelName));
            }
        }

        BeanUtils.copyProperties(model, resultMap);
        return model;
    }

    public static <T> T convertProperties(T model, ParameterTool parameterTool) throws Exception {
        Map<String, String> parametersMap = parameterTool.toMap();
        convertProperties(model, parametersMap);
        return model;
    }


    /**
     * 如果FlinkProperties的部分特殊KV需要手动指定时
     *
     * @param flinkProperties
     * @param parameterTool
     * @return
     */
    public static FlinkProperties setSpecialParameterToFlinkProperties(FlinkProperties flinkProperties, ParameterTool parameterTool) {
        flinkProperties.setEnvType(parameterTool.get(PropertiesConstants.ENV_TYPE));

        // 如果 kafkaBrokers 参数不为空,则 通过kafkaBrokers来设置 kafkaBootstrapServers
        String kafkaBrokers = flinkProperties.getKafkaBrokers();
        if (StringUtils.isNotBlank(kafkaBrokers)) {
            // 通过Kafka配置名获取明细地址
            flinkProperties.setKafkaBootstrapServers(parameterTool.get(kafkaBrokers));
        }

        return flinkProperties;
    }

    public static FlinkProperties getEnvFlinkProperties(StreamExecutionEnvironment env) throws Exception {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        FlinkProperties flinkProperties = convertProperties(new FlinkProperties(), parameterTool);
        setSpecialParameterToFlinkProperties(flinkProperties, parameterTool);
        return flinkProperties;
    }

    public static void setEnvConfig(StreamExecutionEnvironment env, FlinkProperties flinkProperties) {
        setEnvCheckpoint(env, flinkProperties.getChk());
        setEnvRuntimeMode(env, flinkProperties.getMode());
    }

    public static void setEnvCheckpoint(StreamExecutionEnvironment env, Long chk) {
        if (chk != null) {
            env.enableCheckpointing(chk);
        }
    }

    public static void setEnvRuntimeMode(StreamExecutionEnvironment env, String mod) {
        if (RuntimeExecutionMode.STREAMING.name().equalsIgnoreCase(mod)) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        } else if (RuntimeExecutionMode.BATCH.name().equalsIgnoreCase(mod)) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        }
    }
}
