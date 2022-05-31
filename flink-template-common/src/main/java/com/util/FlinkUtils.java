package com.util;

import com.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class FlinkUtils {
    public static StreamExecutionEnvironment createEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static StreamExecutionEnvironment createEnv(long chkInterval) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(chkInterval);
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
}
