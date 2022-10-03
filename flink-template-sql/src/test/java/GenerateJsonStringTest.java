import com.alibaba.fastjson.JSON;
import com.model.UserLogin;
import com.model.UserRecharge;

public class GenerateJsonStringTest {
    // 2022-10-01 12:00:00 -> 1664596800000
    public static void main(String[] args) {
        // user_login.txt
        generateUserLoginJsonString("1001", 1664596800000L);
        generateUserLoginJsonString("1001", 1664596801000L);
        generateUserLoginJsonString("1002", 1664596803000L);
        generateUserLoginJsonString("1003", 1664596805000L);
        generateUserLoginJsonString("1002", 1664596807000L);
        generateUserLoginJsonString("1001", 1664596809000L);

        // user_recharge.txt
        generateUserRechargeJsonString("1001", 1, 100.0, 1664596800000L);
        generateUserRechargeJsonString("1003", 1, 200.0, 1664596801000L);
        generateUserRechargeJsonString("1005", 1, 300.0, 1664596803000L);
        generateUserRechargeJsonString("1007", 1, 400.0, 1664596805000L);
        generateUserRechargeJsonString("1001", 1, 100.0, 1664596807000L);
    }

    public static void generateUserLoginJsonString(String userId, Long ts) {
        UserLogin userLogin = UserLogin.builder()
                .userId(userId)
                .loginTs(ts)
                .build();
        System.out.println(JSON.toJSONString(userLogin));
    }

    public static void generateUserRechargeJsonString(String userId, Integer type, Double amount, Long ts) {
        UserRecharge userRecharge = UserRecharge.builder()
                .userId(userId)
                .rechargeType(type)
                .rechargeAmount(amount)
                .rechargeTs(ts)
                .build();
        System.out.println(JSON.toJSONString(userRecharge));
    }

}
