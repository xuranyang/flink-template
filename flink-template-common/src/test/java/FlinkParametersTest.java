import com.util.FlinkUtils;
import com.util.ParameterConfigUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkParametersTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.createEnv();
        ParameterTool parameterTool = FlinkUtils.getParameterTool(args);

        new ParameterConfigUtils(env, parameterTool).setFlinkParameterConfig();

        env.fromElements("123","456","789").print();

        env.execute();
    }
}
