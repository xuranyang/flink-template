package com.sink;

import com.sink.util.DruidConnectionPool;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.*;
import java.util.List;

/**
 * preCommit -> beginTransaction -> commit -> invoke
 * preCommit -> beginTransaction -> commit -> invoke -> abort -> 从上一个Checkpoint重启 -> recoverAndCommit -> recoverAndAbort
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<List<String>,
        MySqlTwoPhaseCommitSink.ConnectionState, Void> {


    // 定义可用的构造函数
    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(ConnectionState.class, new ExecutionConfig()),
                VoidSerializer.INSTANCE);
    }

    // 开始事务
    @Override
    protected ConnectionState beginTransaction() throws Exception {
        System.out.println("=====> beginTransaction... ");
        // 使用连接池，不使用单个连接
        //Class.forName("com.mysql.jdbc.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test_db?characterEncoding=UTF-8", "root", "123456");

        Connection connection = DruidConnectionPool.getConnection();
        connection.setAutoCommit(false);//设定不自动提交
        return new ConnectionState(connection);
    }


    @Override
    protected void invoke(ConnectionState transaction, List<String> sqlList, Context context) throws Exception {
        System.out.println("=====> invoke... ");
        Connection connection = transaction.connection;

//        PreparedStatement pstmt = connection.prepareStatement("insert into test_table(id,name,age) values(?, ?, ?)");
        Statement stmt = connection.createStatement();

        if (sqlList.size() > 0) {
            for (int i = 0; i < sqlList.size(); i++) {
                String dml_sql = sqlList.get(i);
                if (dml_sql != null) {
                    stmt.addBatch(dml_sql);
                }
            }
        }

        int[] count = stmt.executeBatch();

        stmt.close();

        System.out.println("成功预执行了" + count.length + "行数据");

    }


    // 预提交，提交前的操作，可以在这里执行一些检查或其他操作
    @Override
    protected void preCommit(ConnectionState transaction) throws Exception {
        System.out.println("=====> preCommit... ");
//        System.out.println("=====> preCommit... " + transaction);
    }

    // 提交事务
    @Override
    protected void commit(ConnectionState transaction) {
        System.out.println("=====> commit... ");
        try {
            Connection connection = transaction.connection;
            connection.commit();
            connection.close();
        } catch (Exception e) {
//            throw new RuntimeException("提交事物异常");
            System.out.println("提交事物异常");
        }
    }

    // 回滚事务
    @Override
    protected void abort(ConnectionState transaction) {
        System.out.println("=====> abort... ");
        try {
            Connection connection = transaction.connection;
            connection.rollback();
            connection.close();
        } catch (Exception e) {
//            throw new RuntimeException("回滚事物异常");
            System.out.println("回滚事物异常");
        }
    }

    /**
     * 恢复并且commit事务
     * 用户实现必须保证这个方法最终会成功。如果该方法失败，Flink应用会重启并且重新调用该方法
     * @param transaction
     */
    @Override
    protected void recoverAndCommit(ConnectionState transaction) {
        System.out.println("=====> recoverAndCommit... ");
    }

    /**
     * 恢复并且abort事务
     * @param transaction
     */
    @Override
    protected void recoverAndAbort(ConnectionState transaction) {
        System.out.println("=====> recoverAndAbort... ");
    }

    // 定义建立数据库连接的方法
    public static class ConnectionState {
        private final transient Connection connection;

        public ConnectionState(Connection connection) {
            this.connection = connection;
        }
    }
}