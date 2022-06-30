package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.function.Function;

/**
 * @PackageName:com.atguigu
 * @ClassNmae:Flink_unbouned
 * @Dscription
 * @author:Esaisa
 * @date:2022/6/29 17:01
 */


public class Flink_unbouned {
    public static void main(String[] args) throws Exception {
        //创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //加载流数据
        DataStream<String> dataSteam = env.socketTextStream("hadoop102", 9999);

        //Transfomation
        DataStream<String> flatmapdatastream = dataSteam.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
            }
        });

        DataStream<Tuple2<String, Integer>> mapdatastream = flatmapdatastream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                Tuple2<String, Integer> tuple = new Tuple2<>(s, 1);
                return tuple;
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keybydataStream = mapdatastream.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keybydataStream.sum(1);

        //sink

        sum.print();

        //提交job
        env.execute();


    }
}
