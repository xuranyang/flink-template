import com.source.utils.CommonUtils;

import java.util.Objects;

public class Test {
    public static void main(String[] args) {
        Objects.requireNonNull(CommonUtils.ifNullToString(null));
//        Objects.requireNonNull(null);
//        System.out.println(Objects.isNull(null));

        String s = CommonUtils.ifNullToString(123);
        System.out.println("ifNullToString:" + s);
    }
}
