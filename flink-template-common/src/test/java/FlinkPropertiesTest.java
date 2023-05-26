import com.google.common.base.CaseFormat;
import com.google.common.collect.Maps;
import com.model.FlinkProperties;
import com.util.FlinkUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.Field;
import java.util.Map;

public class FlinkPropertiesTest {
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
            String lowerCamelName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);

            if (parametersMap.containsKey(name)) {
                resultMap.put(name, parametersMap.get(name));
            } else if (!parametersMap.containsKey(name) && parametersMap.containsKey(lowerCamelName)) {
                resultMap.put(name, parametersMap.get(lowerCamelName));
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

    // -envType dev -chk 60000 -kafkaBrokers kafka.brokers
    // -env_type dev -chk 60000 -kafka_brokers kafka.brokers
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = FlinkUtils.createEnv();
        ParameterTool parameterTool = FlinkUtils.getParameterTool(args);
//        FlinkProperties flinkProperties = new FlinkProperties();
//        BeanUtils.copyProperties(flinkProperties, parameterTool.toMap());

        FlinkProperties flinkProperties = convertProperties(new FlinkProperties(), parameterTool);
        System.out.println(flinkProperties);

        System.out.println(parameterTool.get(flinkProperties.getKafkaBrokers()));
    }
}
