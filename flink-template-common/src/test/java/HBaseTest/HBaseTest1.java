package HBaseTest;

import com.util.hbase.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Arrays;

/**
 * create 'test_2023',{NAME=>'cf1',COMPRESSION=>'SNAPPY',TTL=>'86400'}
 * put 'test_2023','rk1','cf1:name','maybe'
 * put 'test_2023','rk1','cf1:age','20'
 * put 'test_2023','rk2','cf1:name','ame'
 *
 * alter 'test_2023',{NAME=>'cf2',COMPRESSION=>'SNAPPY',TTL=>'86400'}
 * put 'test_2023','rk1','cf2:country','china'
 *
 * alter 'test_2023',{NAME=>'cf3',COMPRESSION=>'SNAPPY',TTL=>'86400',VERSIONS=>10}
 * put 'test_2023','rk5','cf3:id','001'
 * put 'test_2023','rk5','cf3:id','002'
 * put 'test_2023','rk5','cf3:id','003'
 *
 * scan 'test_2023'
 */
public class HBaseTest1 {
    public static void main(String[] args) throws InterruptedException {
        HbaseProperties hbaseProperties = new HbaseProperties();
        hbaseProperties.setZkQuorum("fat01,fat02");
//        hbaseProperties.setPoolSize(4);
        hbaseProperties.setPoolSize(1);
        // 是否使用线程池
        hbaseProperties.setUseThreadPool(true);

        HbaseUtils hbaseTemplate = new HbaseAutoConfiguration(hbaseProperties).hbaseTemplate();

//        hbaseTemplate.put("test_2023", "rk3", "cf1", "name", "fy");
//        hbaseTemplate.put("test_2023", "rk4", "cf1", "name", "chalice");
//        hbaseTemplate.put("test_2023", "rk4", "cf1", "age", "19");
//        hbaseTemplate.delete("test_2023", "rk4", "cf1", "name");

//        hbaseTemplate.putMutate("test_2023", "rk3", "cf1", "name", "fy");
//        hbaseTemplate.putMutate("test_2023", "rk4", "cf1", "name", "chalice");
//        hbaseTemplate.putMutate("test_2023", "rk4", "cf1", "age", "19");
//        hbaseTemplate.deleteMutate("test_2023", "rk4", "cf1", "name");

        System.out.println(hbaseTemplate.get("test_2023", "rk1", "cf1", "name"));
        System.out.println("==================");
        System.out.println(hbaseTemplate.getBatch("test_2023", Arrays.asList("rk1", "rk2", "rk3"), "cf1", "name"));
        System.out.println("==================");
        System.out.println(hbaseTemplate.getBatch("test_2023", Arrays.asList("rk1", "rk2", "rk3"), "cf1"));
        System.out.println("==================");
        System.out.println(hbaseTemplate.getBatch("test_2023", Arrays.asList("rk1", "rk2", "rk3")));
        System.out.println("==================");
        System.out.println(hbaseTemplate.getBatch("test_2023", Arrays.asList("rk1", "rk2", "rk3"), new MapRowMapper()));
        System.out.println("==================");
        System.out.println(hbaseTemplate.getBatch("test_2023", Arrays.asList("rk1", "rk5"), new VersionMapRowMapper()));
        hbaseTemplate.closeConnection();
    }
}
