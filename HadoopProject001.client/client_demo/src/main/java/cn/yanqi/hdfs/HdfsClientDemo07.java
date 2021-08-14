package cn.yanqi.hdfs;
/*
    遍历hdfs的根目录得到文件以及文件夹的信息：名称，权限，长度等
 */

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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientDemo07 {
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
    //下载文件
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

        //boolean:是否删除源文件
        //src：hdfs路径
        //dst：目标路径，本地路径
        fs.copyToLocalFile(true, new Path("/yanqi.txt"), new Path("e:/yanqi_copy.txt"), true);

    }

    //删除文件或者文件夹
    @Test
    public void deleteFile() throws IOException, InterruptedException, URISyntaxException{

        fs.delete(new Path("/api_test2/"), true);

    }


    //遍历hdfs的根目录得到文件以及文件夹的信息：名称，权限，长度等
    @Test
    public void ListFiles() throws IOException, InterruptedException, URISyntaxException{
        //得到一个迭代器；装有指定目录下所有文件信息
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"),true);

        while(remoteIterator.hasNext()) {
            LocatedFileStatus filestatus = remoteIterator.next();
            //输出详情
            //文件名称
            final String fileName = filestatus.getPath().getName();
            //长度
            final long len = filestatus.getLen();
            //权限
            final FsPermission permission = filestatus.getPermission();
            //分组
            final String group = filestatus.getGroup();
            //用户
            final String owner = filestatus.getOwner();

            System.out.println(fileName + "\t"  + len + "\t" + permission + "\t" + group + "\t" + owner);
            //块信息
            final BlockLocation[] blockLocations = filestatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations){
                final String[] hosts = blockLocation.getHosts();
                for (String host : hosts){
                    System.out.println( "主机名称"+host );
                }
            }
            System.out.println("-----------华丽的分割线----------");

        }
    }

}