package com.sink.util;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;

public class JdbcUtil {

    public static Connection createDruidConnectionPool(String host, String port, String userName, String passWord, String database) {
        // 数据源配置
        DruidDataSource dataSource = new DruidDataSource();
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?serverTimezone=GMT%2B3";
        dataSource.setUrl(url);
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUsername(userName);
        dataSource.setPassword(passWord);

        // 下面都是可选的配置
        dataSource.setInitialSize(2);  //初始连接数，默认0
        dataSource.setMaxActive(4);  //最大连接数，默认8
        dataSource.setMinIdle(2);  //最小闲置数
        dataSource.setMaxEvictableIdleTimeMillis(7 * 24 * 3600 * 1000); //一个连接在池中最大生存的时间(ms)
//        dataSource.setMaxWait(2000);  //获取连接的最大等待时间，单位毫秒
//        dataSource.setPoolPreparedStatements(true); //缓存PreparedStatement，默认false
//        dataSource.setMaxOpenPreparedStatements(20); //缓存PreparedStatement的最大数量，默认-1（不缓存）。大于0时会自动开启缓存PreparedStatement，所以可以省略上一句代码

        Connection connection = null;
        try {
            connection = dataSource.getConnection();
//            System.out.println("创建连接池：" + connection);
            System.out.println("[Create MySQL DruidConnectionPool Success]:" + url);
        } catch (Exception e) {
            System.out.println("-----MySQL get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }

    public static Connection createDruidConnectionPool(String host, String port, String userName, String passWord, String database, String timeZone) {
        // 数据源配置
        DruidDataSource dataSource = new DruidDataSource();
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?serverTimezone=" + timeZone;
        dataSource.setUrl(url);
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUsername(userName);
        dataSource.setPassword(passWord);

        // 下面都是可选的配置
        dataSource.setInitialSize(2);  //初始连接数，默认0
        dataSource.setMaxActive(4);  //最大连接数，默认8
        dataSource.setMinIdle(2);  //最小闲置数
        dataSource.setMaxEvictableIdleTimeMillis(7 * 24 * 3600 * 1000); //一个连接在池中最大生存的时间(ms)
//        dataSource.setMaxWait(2000);  //获取连接的最大等待时间，单位毫秒
//        dataSource.setPoolPreparedStatements(true); //缓存PreparedStatement，默认false
//        dataSource.setMaxOpenPreparedStatements(20); //缓存PreparedStatement的最大数量，默认-1（不缓存）。大于0时会自动开启缓存PreparedStatement，所以可以省略上一句代码

        Connection connection = null;
        try {
            connection = dataSource.getConnection();
//            System.out.println("创建连接池：" + connection);
            System.out.println("[Create MySQL DruidConnectionPool Success]:" + url);
        } catch (Exception e) {
            System.out.println("-----MySQL get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }

}
