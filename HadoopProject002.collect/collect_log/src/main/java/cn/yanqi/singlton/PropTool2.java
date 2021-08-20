package cn.yanqi.singlton;

import com.yanqi.collect.LogCollectorTask;

import java.io.IOException;
import java.util.Properties;

public class PropTool2 {

    //volatile关键字是java中禁止指令重排序的关键字，保证有序性和可见性
    private static volatile Properties prop=null;



    //出现线程安全问题
    public static  Properties getProp() throws IOException {
        if(prop ==null){
            synchronized ("lock"){
                if(prop ==null){
                    prop=new Properties();
                    prop.load(LogCollectorTask.class.getClassLoader()
                            .getResourceAsStream("collector.properties"));
                }
            }
        }

        return prop;
    }

}
