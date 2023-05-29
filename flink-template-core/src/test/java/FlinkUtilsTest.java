import com.util.FlinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;

public class FlinkUtilsTest {
    /**
     * Pro:
     * kafka.brokers=kafka01:9092,kafka02:9092,kafka03:9092
     * kafka.group.id=kafka-group-1
     * hbase.zookeeper.quorum=hbase01,hbase02,hbase03
     * hbase.zookeeper.property.clientPort=2181
     *
     * Dev:
     * kafka.brokers=test.kafka01:9092,test.kafka02:9092,test.kafka03:9092
     * kafka.group.id=test-kafka-group-1
     * hbase.zookeeper.quorum=test.hbase01,test.hbase02,test.hbase03
     * hbase.zookeeper.property.clientPort=2181
     */
    public static void main(String[] args) throws Exception {
        // --env_type pro or --env_type dev
        ParameterTool parameterTool = FlinkUtils.getParameterTool(args);
        String kafkaBrokers = parameterTool.get("kafka.brokers");
        String kafkaGroupId = parameterTool.get("kafka.group.id");
        String hbaseZk = parameterTool.get("hbase.zookeeper.quorum");
        String hbaseZkPort = parameterTool.get("hbase.zookeeper.property.clientPort");
        System.out.println(kafkaBrokers);
        System.out.println(kafkaGroupId);
        System.out.println(hbaseZk);
        System.out.println(hbaseZkPort);
    }
}
