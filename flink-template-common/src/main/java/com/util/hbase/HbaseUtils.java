package com.util.hbase;

import com.constant.PropertiesConstants;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HbaseUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseUtils.class);

    private Configuration configuration;

    private volatile Connection connection;
    private ThreadPoolExecutor poolExecutor;
    private Boolean useThreadPool;

    public HbaseUtils(Configuration configuration) {
        this.setConfiguration(configuration);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        useThreadPool = configuration.getBoolean(PropertiesConstants.HBASE_PARAM_USE_THREAD_POOL, false);
        if (useThreadPool) {
            return getConnectionPool();
        } else {
            return getConnectionSingle();
        }
    }

    public Connection getConnectionPool() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                        int hbasePoolSize = configuration.getInt(PropertiesConstants.HBASE_PARAM_POOL_SIZE, PropertiesConstants.HBASE_PARAM_POOL_SIZE_DEFAULT);
                        poolExecutor = new ThreadPoolExecutor(hbasePoolSize, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadFactoryBuilder().setNameFormat("HBase-Pool-Thread-%d").build());
                        // init pool
                        poolExecutor.prestartCoreThread();
                        this.connection = ConnectionFactory.createConnection(configuration, poolExecutor);
                        LOGGER.info("HBase资源池创建成功,资源池大小:{}", hbasePoolSize);
                    } catch (IOException e) {
                        LOGGER.error("site.assad.spring.data.hbase connection资源池创建失败");
                    }
                }
            }
        }
        return this.connection;
    }

    public Connection getConnectionSingle() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                        this.connection = ConnectionFactory.createConnection(configuration);
                        LOGGER.info("HBase连接创建成功");
                    } catch (Exception e) {
                        LOGGER.error("HBase连接创建失败:{}", e.getMessage());
                    }
                }
            }
        }
        return this.connection;
    }

    public void closeConnection() {
        if (useThreadPool) {
            closeConnectionSingle();
            closeConnectionPool();
        } else {
            closeConnectionSingle();
        }
    }

    public void closeConnectionSingle() {
        try {
            this.connection.close();
            LOGGER.info("HBase连接关闭成功");
        } catch (Exception e) {
            LOGGER.error("HBase连接关闭失败:{}", e.getMessage());
        }
    }

    public void closeConnectionPool() {
        try {
            poolExecutor.shutdown();
            LOGGER.info("HBase线程池关闭成功");
        } catch (Exception e) {
            LOGGER.error("HBase线程池关闭失败:{}", e.getMessage());
        }
    }

    public <T> T execute(String tableName, TableCallback<T> action) {
        Table table = null;
        try {
            table = this.getConnection().getTable(TableName.valueOf(tableName));
            return action.doInTable(table);
        } catch (Throwable throwable) {
            LOGGER.error("HBase执行失败原因:{}", throwable.getCause().toString());
            LOGGER.error("HBase执行失败信息:{}", throwable.getMessage());
        } finally {
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    LOGGER.error("HBase资源释放失败:{}", e.getMessage());
                }
            }
        }
        return null;
    }

    public void execute(String tableName, MutatorCallback action) {
        BufferedMutator mutator = null;
        try {
            BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName));
            mutator = this.getConnection().getBufferedMutator(mutatorParams.writeBufferSize(PropertiesConstants.HBASE_WRITE_BUFFER_SIZE));
            action.doInMutator(mutator);
        } catch (Throwable throwable) {
            LOGGER.error("HBase执行失败原因:{}", throwable.getCause().toString());
            LOGGER.error("HBase执行失败信息:{}", throwable.getMessage());
        } finally {
            if (null != mutator) {
                try {
                    mutator.flush();
                    mutator.close();
                } catch (IOException e) {
                    LOGGER.error("hbase mutator资源释放失败");
                }
            }
        }
    }

    public void putMutate(String tableName, final String rowKey, final String columnFamily, final String qualifier, String value) {
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                mutator.mutate(put);
            }
        });

    }

    public void deleteMutate(String tableName, final String rowKey, final String columnFamily, final String qualifier) {
        this.execute(tableName, mutator -> {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            mutator.mutate(delete);
        });

    }

    public void put(String tableName, final String rowKey, final String columnFamily, final String qualifier, String value) {
        this.execute(tableName, table -> {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            return true;
        });
    }

    public void delete(String tableName, final String rowKey, final String columnFamily, final String qualifier) {
        this.execute(tableName, table -> {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            table.delete(delete);
            return true;
        });
    }

    /**
     * get 'test_2023','rk1','cf1:name'
     * get <table>,<rowkey>,[<family:column>,....]
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier
     * @return
     */
    public String get(String tableName, final String rowKey, final String columnFamily, final String qualifier) {
        return this.execute(tableName, new TableCallback<String>() {
            @Override
            public String doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowKey));

                if (StringUtils.isNotBlank(columnFamily)) {
                    byte[] cf = Bytes.toBytes(columnFamily);
                    if (StringUtils.isNotBlank(qualifier)) {
                        get.addColumn(cf, Bytes.toBytes(qualifier));
                    } else {
                        get.addFamily(cf);
                    }
                }
                Result result = table.get(get);

                return Bytes.toString(result.getValue(columnFamily.getBytes(), qualifier.getBytes()));
            }
        });
    }

    public List<Map<String, String>> getBatch(String tableName, List<String> rowKeyList) {

        return this.execute(tableName, table -> {
            List<Map<String, String>> resList = new ArrayList<>();

            List<Get> getList = new ArrayList<>();
            for (String rowKey : rowKeyList) {
                // 排除null值和空值
                if (Objects.isNull(rowKey) || StringUtils.isBlank(rowKey)) {
                    continue;
                }
                Get get = new Get(Bytes.toBytes(rowKey));
                getList.add(get);
            }

            Result[] results = new Result[getList.size()];
            table.batch(getList, results);

            for (Result result : results) {
                Map<String, String> cells = new HashMap<>();

                for (Cell cell : result.rawCells()) {
                    String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                    String q = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String v = Bytes.toString(CellUtil.cloneValue(cell));
                    cells.put(cf + ":" + q, v);
                }
                resList.add(cells);
            }

            return resList;
        });
    }

    public List<Map<String, String>> getBatch(String tableName, List<String> rowKeyList, String columnFamily) {
        byte[] family = Bytes.toBytes(columnFamily);

        return this.execute(tableName, table -> {
            List<Map<String, String>> resList = new ArrayList<>();

            List<Get> getList = new ArrayList<>();
            for (String rowKey : rowKeyList) {
                // 排除null值和空值
                if (Objects.isNull(rowKey) || StringUtils.isBlank(rowKey)) {
                    continue;
                }
                Get get = new Get(Bytes.toBytes(rowKey));

                if (StringUtils.isNotBlank(columnFamily)) {
                    get.addFamily(family);
                }

                getList.add(get);
            }

            Result[] results = new Result[getList.size()];
            table.batch(getList, results);

            for (Result result : results) {
                Map<String, String> cells = new HashMap<>();

                for (Cell cell : result.rawCells()) {
                    String q = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String v = Bytes.toString(CellUtil.cloneValue(cell));
                    cells.put(q, v);
                }
                resList.add(cells);
            }

            return resList;
        });
    }

    public List<String> getBatch(String tableName, List<String> rowKeyList, String columnFamily, String qualifier) {
        return this.execute(tableName, table -> {
            List<String> resList = new ArrayList<>();

            List<Get> getList = new ArrayList<>();
            for (String rowKey : rowKeyList) {
                // 排除null值和空值
                if (Objects.isNull(rowKey) || StringUtils.isBlank(rowKey)) {
                    continue;
                }
                Get get = new Get(Bytes.toBytes(rowKey));
                getList.add(get);
            }

            Result[] results = new Result[getList.size()];
            table.batch(getList, results);

            for (Result result : results) {
                String cellData = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier)));
                resList.add(cellData);
            }

            return resList;
        });
    }

    public <T> List<T> getBatch(String tableName, List<String> rowKeyList, RowMapper<T> mapper) {
        return this.execute(tableName, table -> {
            //声明一个数组来保存所有的操作
            List<Row> batch = new ArrayList<>();
            table = connection.getTable(TableName.valueOf(tableName));// 获取表
            for (String rowKey : rowKeyList) {
                Get get = new Get(Bytes.toBytes(rowKey));
                batch.add(get);
            }
            Result[] results = new Result[batch.size()];
            table.batch(batch, results);
            List<T> dtos = new ArrayList<>();
            for (int i = 0; i < results.length; i++) {
                dtos.add(mapper.mapRow(results[i], i));
            }
            return dtos;
        });
    }

}
