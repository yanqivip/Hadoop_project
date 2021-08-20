package cn.yanqi.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientDemo12 {
    FileSystem fs = null;
    Configuration configuration = null;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1、获取Hadoop集群的configuration对象
        configuration = new Configuration();
        //configuration.set( "fs.defaultFS", "hdfs://linux121:9000" );
        //configuration.set( "dfs.replication", "2" );

        //2、根据configuration获取Filesystem对象
        fs = FileSystem.get( new URI( "hdfs://linux121:9000" ), configuration, "root" );
    }


    @After
    public void destory() throws IOException {
        //4、释放filesystem对象(类似数据库连接)
        fs.close();
    }


    //验证packet代码
    @Test
    public void testUploadPacket() throws IOException {
        //1 准备读取本地文件的输入流
        final FileInputStream in = new FileInputStream(new  File("e:/yanqi.txt"));
        //2 准备好写出数据到hdfs的输出流
        final FSDataOutputStream out = fs.create(new Path("/yanqi.txt"), new Progressable() {
            public void progress() { //这个progress方法就是每传输64KB（packet）就会执行一次，
                System.out.println("&");
            }
        });
        //3 实现流拷贝
        IOUtils.copyBytes(in, out, configuration); //默认关闭流选项是true，所以会自动关闭

        //4 关流 可以再次关闭也可以不关了
    }

}