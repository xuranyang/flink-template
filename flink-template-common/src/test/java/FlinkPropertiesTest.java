import com.model.FlinkKafkaProperties;
import com.model.FlinkProperties;
import com.model.KafkaRecord;
import com.schema.KafkaRecordSchema;
import com.util.FlinkUtils;
import com.util.flink.FlinkKafkaUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkPropertiesTest {

    /*
     *  启动参数传入参考:
        -envType fat -chk 60000 -globalStrategy earliest -kafkaBrokers kafka.brokers -kafkaTopics Data_Test_1 -kafkaGroupId test_group -kafkaOffsets 0:15
        -env_type fat -chk 60000 -global_strategy special -kafka_brokers kafka.brokers -kafka_topics Data_Test_1 -kafka_group_id test_group -kafka_offsets 0:15
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.createEnv();
        ParameterTool parameterTool = FlinkUtils.getParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

//        FlinkProperties flinkProperties = new FlinkProperties();
//        BeanUtils.copyProperties(flinkProperties, parameterTool.toMap());

//        FlinkProperties flinkProperties = FlinkUtils.convertProperties(new FlinkProperties(), parameterTool);
//        FlinkUtils.setSpecialParameterToFlinkProperties(flinkProperties, parameterTool);

        FlinkProperties flinkProperties = FlinkUtils.getEnvFlinkProperties(env);
        FlinkUtils.setEnvConfig(env, flinkProperties);
//        System.out.println(flinkProperties);
//        System.out.println(parameterTool.get(flinkProperties.getKafkaBrokers()));

        FlinkKafkaProperties flinkKafkaProperties = FlinkKafkaProperties.builder()
                .kafkaStrategy("earliest")
                .kafkaGroupId("test-group")
                .kafkaTopics("Data_Test_1")
                .kafkaBrokers("kafka.brokers")
//                .kafkaBootstrapServers("test-kafka:9092")
                .build();

//        DataStreamSource<String> kafkaSource = FlinkKafkaUtils.getKafkaSource(env, flinkProperties);
//        DataStreamSource<String> kafkaSource = FlinkKafkaUtils.getKafkaSource(env, flinkKafkaProperties);

        DataStreamSource<KafkaRecord> kafkaSource = FlinkKafkaUtils.getKafkaSource(env, flinkProperties, new KafkaRecordSchema());
//        DataStreamSource<KafkaRecord> kafkaSource = FlinkKafkaUtils.getKafkaSource(env, flinkKafkaProperties, new KafkaRecordSchema());

        kafkaSource.print();

        env.execute();
    }
}
