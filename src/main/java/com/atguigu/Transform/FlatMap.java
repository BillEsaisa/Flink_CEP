package com.atguigu.Transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:FlatMap
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 6:56
 */


public class FlatMap {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从网络端口读取数据、
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //TODO 对读取到数据进行flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.split(",");
                Tuple2<String, Integer> tuple2 = new Tuple2<>();
                for (String string : strings) {
                    tuple2.setFields(string,1);
                    collector.collect(tuple2);
                }

            }
        });

        //打印流
        flatMap.print();

        //提交job
        env.execute();

    }

}
