import com.source.utils.FlinkUtils;

import java.util.Objects;

public class Test {
    public static void main(String[] args) {
        Objects.requireNonNull(FlinkUtils.ifNullToString(null));
//        Objects.requireNonNull(null);
//        System.out.println(Objects.isNull(null));

        String s = FlinkUtils.ifNullToString(123);
        System.out.println("ifNullToString:" + s);
    }
}
