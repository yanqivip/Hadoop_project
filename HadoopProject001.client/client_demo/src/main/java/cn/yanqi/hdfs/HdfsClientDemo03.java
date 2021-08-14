package cn.yanqi.hdfs;
/*
    获取filesystem对象方案三：推荐
    HDFS之API客户端解决文件权限问题
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientDemo03 {
    FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1、获取Hadoop集群的configuration对象
        Configuration configuration = new Configuration();
        configuration.set( "fs.defaultFS", "hdfs://linux121:9000" );


        //2、根据configuration获取Filesystem对象
        fs = FileSystem.get( new URI( "hdfs://linux121:9000" ), configuration, "root" );
    }


    @After
    public void destory() throws IOException {
        //4、释放filesystem对象(类似数据库连接)
        fs.close();
    }

    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {


        //FileSystem fs  = FileSystem.get(configuration);
        //3、使用filesystem对象创建一个测试
        fs.mkdirs( new Path( "/api_test2" ) );
    }
}