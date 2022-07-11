package com.atguigu.Transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:Reduce
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 10:22
 *
 * reduce ；聚合操作，这是一个比较底层的聚合操作算子，它能实现比较多的聚合操作
 * 因为reduce的聚合有状态，它能把上一次聚合结果（上一次的状态保存），进而与下一条的数据进行聚合
 *
 * 注意：
 * 在第一条数据不会进入reduce方法，因为在第一条数据之前并没有保存的以前的状态，以前的聚合结果，
 *
 *
 */


public class Reduce {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从网络端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //我们拿reduce做一个sum相似的功能
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                Tuple2<String, Integer> tuple2 = new Tuple2<>();
                for (String s2 : s1) {
                    tuple2.setFields(s2,1);
                    collector.collect(tuple2);
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {

                return new Tuple2<>(stringIntegerTuple2.f0,stringIntegerTuple2.f1+t1.f1);
            }
        }).print();

        //提交job
        env.execute();
    }
}
