package cn.yanqi.collect;

import com.yanqi.common.Constant;
import com.yanqi.singlton.PropTool2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

public class LogCollectorTask extends TimerTask {

    @Override
    public void run() {

        Properties prop = null;
        try {
            prop = PropTool2.getProp();
        } catch (IOException e) {
            e.printStackTrace();
        }


        //采集的业务逻辑
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String todayStr = sdf.format(new Date());
        // 1 扫描指定目录，找到待上传文件,原始日志目录
        File logsDir = new File(prop.getProperty(Constant.LOGS_DIR));
        final String log_prefix = prop.getProperty(Constant.LOG_PREFIX);
        File[] uploadFiles = logsDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(log_prefix);
            }
        });
        //2 把待上传文件转移到临时目录
        //判断临时目录是否存在，
        File tmpFile = new File(prop.getProperty(Constant.LOG_TMP_FOLDER));
        if (!tmpFile.exists()) {
            tmpFile.mkdirs();
        }
        for (File file : uploadFiles) {
            file.renameTo(new File(tmpFile.getPath() + "/" + file.getName()));
        }

        //3 使用hdfs api上传日志文件到指定目录
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://linux121:9000");

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            //判断hdfs目标路径是否存在，备份目录是否存在
            Path path = new Path(prop.getProperty(Constant.HDFS_TARGET_FOLDER) + todayStr);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            File bakFolder = new File(prop.getProperty(Constant.BAK_FOLDER) + todayStr);
            if (!bakFolder.exists()) {
                bakFolder.mkdirs();
            }
            File[] files = tmpFile.listFiles();
            for (File file : files) {
                //按照日期分门别列存放
                fs.copyFromLocalFile(new Path(file.getPath()), new Path(prop.getProperty(Constant.HDFS_TARGET_FOLDER) + todayStr + "/" + file.getName()));
                //4 上传后的文件转移到备份目录
                file.renameTo(new File(bakFolder.getPath() + "/" + file.getName()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
