import com.source.utils.FlinkUtils;

public class UtilsTestMain {
    private static FlinkUtils flinkUtils = new FlinkUtils();

    public static void main(String[] args) {
        String s1 = "hello";
        String s2 = null;
        Integer i1 = 24;
        Integer i2 = null;
        System.out.println(flinkUtils.ifNullToString(s1));
        System.out.println(flinkUtils.ifNullToString(s2));
        System.out.println(flinkUtils.ifNullToString(i1));
        System.out.println(flinkUtils.ifNullToString(i2));

    }
}
