package com.source;

import com.model.UserInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLSource extends RichSourceFunction<Map<String, UserInfo>> {
    private boolean flag = true;
    private Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;

    /**
     * CREATE TABLE `user_info` (
     * `userID` varchar(255) NOT NULL,
     * `userName` varchar(255) DEFAULT NULL,
     * `age` int(11) DEFAULT NULL,
     * `nationality` varchar(255) DEFAULT NULL,
     * PRIMARY KEY (`userID`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     * <p>
     * INSERT INTO `user_info` VALUES ('maybe', 'Luyao', 26,'CN');
     * INSERT INTO `user_info` VALUES ('ame', 'WangChunyu', 24,'CN');
     * INSERT INTO `user_info` VALUES ('fy', 'ZhangChengjun', 26,'CN');
     * INSERT INTO `user_info` VALUES ('eurus', 'XuLinsen', 26,'CN');
     * INSERT INTO `user_info` VALUES ('yang', 'ZhouHaiyang', 25,'CN');
     * INSERT INTO `user_info` VALUES ('emo', 'ZhoYi', 20,'CN');
     * INSERT INTO `user_info` VALUES ('kaka', 'HuLiangzhi', 28,'CN');
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
                "root", "root00");
        String sql = "select `userID`, `userName`, `age`,`nationality` from `user_info`";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Map<String, UserInfo>> ctx) throws Exception {
        while (flag) {
            HashMap<String, UserInfo> userMap = new HashMap<>();
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String userID = rs.getString("userID");
                String userName = rs.getString("userName");
                int age = rs.getInt("age");
                String nationality = rs.getString("nationality");
                userMap.put(userID, new UserInfo(userID, userName, age, nationality));

            }
            ctx.collect(userMap);
            Thread.sleep(10000);//每隔10s更新一下用户的配置信息!
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        if (conn != null) conn.close();
        if (ps != null) ps.close();
        if (rs != null) rs.close();
    }

}
