package com.atguigu.Transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:SimpleAggOperator
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 8:55
 *
 * 简单的聚合算子，切记聚合之前必须要keyby
 * 以sum为例
 *
 */


public class SimpleAggOperator_sum {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从网络端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        // 对输入数据进行Flatmap,keyby.sum 操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                Tuple2<String, Integer> tuple2 = new Tuple2<>();
                for (String s2 : s1) {
                    tuple2.setFields(s2, 1);
                    collector.collect(tuple2);
                }
            }
        }).keyBy(0).sum(1);

        //打印流
        sum.print();

        //提交job
        env.execute();



    }

}
