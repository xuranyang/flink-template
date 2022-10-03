
import com.util.FlinkUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkKafkaProducerTest {
    public static String KAFKA_BROKERS = "test-kafka:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.createEnv();
        String topic = "test_topic";
//        DataStreamSource<String> dataStreamSource = env.fromElements("c:hello", "a:world", "b:flink", "d:bigdata", "a:hi", "b:waow");

        DataStreamSource<String> dataStreamSource = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true){
                    sourceContext.collect("kafka_key:kafka_value");
                    TimeUnit.SECONDS.sleep(3);
                }
            }
            @Override
            public void cancel() {

            }
        });
//        dataStreamSource.print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BROKERS);
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_BROKERS);

//        dataStreamSource.print();
        dataStreamSource.addSink(new FlinkKafkaProducer<String>("default_kafka_topic",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                        byte[] key = s.split(":")[0].getBytes();
                        byte[] value = s.split(":")[1].getBytes();
                        return new ProducerRecord<byte[], byte[]>(topic, key, value);
                    }
                }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute();
    }
}
