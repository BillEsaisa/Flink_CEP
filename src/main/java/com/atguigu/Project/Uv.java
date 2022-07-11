package com.atguigu.Project;

import com.atguigu.Bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:Uv
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 23:46
 *
 * 网站的独立访客数
 */


public class Uv {
    public static void main(String[] args) throws Exception {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\UserBehavior.csv");

        //转换为样例类
        SingleOutputStreamOperator<UserBehavior> map = stringDataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.valueOf(split[2]), split[3], Long.parseLong(split[4]));
            }
        });

        //过滤
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });
        //keyby
        KeyedStream<UserBehavior, Tuple> keyBy = filter.keyBy("behavior");


        //去重
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = keyBy.process(new ProcessFunction<UserBehavior, Tuple2<String, Integer>>() {
            HashSet<Long> uids = new HashSet<>();

            @Override
            public void processElement(UserBehavior value, ProcessFunction<UserBehavior, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                uids.add(value.getUserId());
                int size = uids.size();
                out.collect(Tuple2.of("uv", Integer.valueOf(size)));

            }


        });

        //打印流
        pv.print().setParallelism(1);

        //提交job
        env.execute();


    }
}
