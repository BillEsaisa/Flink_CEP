package com.atguigu.Transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:Shuffle
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 8:19
 *
 * shuffle 是对数据进行随机的打散
 * 数据量越大，散列越好
 * 源码中用的是random 实现
 *
 *
 */


public class Shuffle {
    public static void main(String[] args) throws Exception {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //网络端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //将数据随机打散到下游（shuffle）
        DataStream<String> shuffle = socketTextStream.shuffle();

        //打印流
        shuffle.print();

        //提交job
        env.execute();


    }
}
