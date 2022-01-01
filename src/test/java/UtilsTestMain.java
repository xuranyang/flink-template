import com.source.utils.FlinkUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class UtilsTestMain {

    public static void main(String[] args) {
        String s1 = "hello";
        String s2 = null;
        Integer i1 = 24;
        Integer i2 = null;
        System.out.println(FlinkUtils.ifNullToString(s1));
        System.out.println(FlinkUtils.ifNullToString(s2));
        System.out.println(FlinkUtils.ifNullToString(i1));
        System.out.println(FlinkUtils.ifNullToString(i2));

    }

    @Test
    public void testListUtils(){
        List<String> stringList = new ArrayList<>();
        stringList.add("a");
        stringList.add("b");
        stringList.add("c");

        List<Integer> integerList = new ArrayList<>();
        integerList.add(1);
        integerList.add(2);
        integerList.add(3);

        String stringListFisrt = FlinkUtils.getListFisrt(stringList);
        System.out.println(stringListFisrt);

        Integer integerListFirst = FlinkUtils.getListFisrt(integerList);
        System.out.println(integerListFirst);

        System.out.println("-----------------");

        int topN = 2;
        List<String> stringListTopN = FlinkUtils.getListTopN(stringList, topN);
        List<Integer> integerListTopN = FlinkUtils.getListTopN(integerList, topN);
        System.out.println(stringListTopN);
        System.out.println(integerListTopN);
    }
}
