package cn.yanqi.hdfs;
/*
    通过API客户端上传文件到API集群：并配置副本数量
    方案一：通过默认方案配置：//configuration.set( "fs.defaultFS", "hdfs://linux121:9000" );
    方案二：通过replication配置参数：//configuration.set("dfs.replication", "2");
    方案三：通过hdfs-site.xml配置文件配置
    方案四：配置优先级：//configuration.set("dfs.replication", "2");高于 hdfs-site.xml配置文件
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

public class HdfsClientDemo04 {
    FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1、获取Hadoop集群的configuration对象
        Configuration configuration = new Configuration();
        //configuration.set( "fs.defaultFS", "hdfs://linux121:9000" );
        configuration.set("dfs.replication", "2");

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
    //上传文件
    @Test
    public void copyFormLocalToHdfs() throws IOException, InterruptedException, URISyntaxException {

        //上传文件
        //src源文件目录：本地路径
        //dst：目标文件目录，hdfs路径
        fs.copyFromLocalFile(new Path("e:/yanqi.txt"), new Path("/yanqi.txt"));
        //上传文件到hdfs默认是3个副本
        //如何改变上传文件的副本数量

    }

}