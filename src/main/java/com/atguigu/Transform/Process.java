package com.atguigu.Transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:Process
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 10:40
 * 
 * 底层算子：process
 * 
 */


public class Process {
    public static void main(String[] args) {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //从集合读取数据
        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5, 6);
        
        //使用process算子
        fromElements.process(new ProcessFunction<Integer, Object>() {
            @Override
            public void processElement(Integer integer, ProcessFunction<Integer, Object>.Context context, Collector<Object> collector) throws Exception {

            }
        });
    }
}
