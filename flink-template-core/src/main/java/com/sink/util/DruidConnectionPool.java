package com.sink.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DruidConnectionPool {
    private transient static DataSource dataSource = null;
    private transient static Properties props = new Properties();

    // 静态代码块
    static {
        props.put("driverClassName", "com.mysql.cj.jdbc.Driver");
        props.put("url", "jdbc:mysql://localhost:3306/test_db?serverTimezone=GMT&characterEncoding=utf8");
        props.put("username", "root");
        props.put("password", "root00");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DruidConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
