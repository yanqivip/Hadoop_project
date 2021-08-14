package com.yanqi.singlton;

import com.yanqi.collect.LogCollectorTask;

import java.io.IOException;
import java.util.Properties;

public class PropTool {
    //类加载时初始化执行一次即可
    //使用静态代码块实现 饿汉式加载
    private static Properties prop=null;

    static {
        prop=new Properties();
        try {
            prop.load(LogCollectorTask.class.getClassLoader().getResourceAsStream("collector.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static  Properties getProp(){
        return prop;
    }

}
