import com.model.FlinkProperties;
import com.util.FlinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;

public class FlinkPropertiesTest {

    // -envType dev -chk 60000 -kafkaBrokers kafka.brokers
    // -env_type dev -chk 60000 -kafka_brokers kafka.brokers
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = FlinkUtils.getParameterTool(args);

//        StreamExecutionEnvironment env = FlinkUtils.createEnv();
//        FlinkProperties flinkProperties = new FlinkProperties();
//        BeanUtils.copyProperties(flinkProperties, parameterTool.toMap());

        FlinkProperties flinkProperties = FlinkUtils.convertProperties(new FlinkProperties(), parameterTool);
        FlinkUtils.setSpecialParameterToFlinkProperties(flinkProperties,parameterTool);

        System.out.println(flinkProperties);
        System.out.println(parameterTool.get(flinkProperties.getKafkaBrokers()));
    }
}
