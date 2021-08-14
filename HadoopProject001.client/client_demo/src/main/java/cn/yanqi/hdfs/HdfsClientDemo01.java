package cn.yanqi.hdfs;
/*
    获取filesystem对象方案一
    HDFS之API客户端解决文件权限问题
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClientDemo01 {
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        //1、获取Hadoop集群的configuration对象
        Configuration configuration = new Configuration();

        //2、根据configuration获取Filesystem对象
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");

        //3、使用filesystem对象创建一个测试
        fs.mkdirs(new Path("/api_test"));
        //4、释放filesystem对象(类似数据库连接)
        fs.close();
    }
}