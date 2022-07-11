package com.atguigu.Transform;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:KeyBy
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 7:50
 *
 * Keyby:将具有相同key的数据划分到一组，组是个逻辑概念，然后按组进行分区
 * 只要有聚合操作，必然要先经过keyby
 * 后续的集聚操作，是针对key相同的数据进行聚合，也就是同一组的数据
 * 不同的分区存在不同的key,所以针对分区聚合是个错误的概念
 * 聚合是对同一组的数据进行的聚合
 * 在flink源码中，对分组做了大量的工作，通过对数据的key 采用hashcode算法，为了减少hash碰撞减少不同的key hash值相同的情况，作者又对hashcode,采用了murmurhash算法
 * 两次hash之后对最大并行度进行取模，默认最大并行度128
 * ？？？不同的key得到不同的hash 但是也可能会存在对128取模之后进入同一组的情况
 *
 */


public class KeyBy {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从网络端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //TODO 将数据格式转化成sensor类，然后将sensorid 作为key,进行分组分区
        KeyedStream<Sensor, Tuple> keyBy = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return new Sensor(s1[0], Long.parseLong(s1[1]), Double.parseDouble(s1[2]));
            }
        }).keyBy("id");

        //打印流
        keyBy.print();

        //提交job
        env.execute();


    }

}
