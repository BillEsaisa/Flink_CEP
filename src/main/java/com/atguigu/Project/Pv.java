package com.atguigu.Project;

import com.atguigu.Bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:Pv
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 23:27
 *
 *
 * 网站页面总浏览数统计
 */


public class Pv {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取日志
        DataStreamSource<String> readTextFile = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\UserBehavior.csv");

        //将数据包装成javaBean
        SingleOutputStreamOperator<UserBehavior> map = readTextFile.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.valueOf(split[2]), split[3], Long.parseLong(split[4]));
            }
        });

        //过滤出页面浏览数据
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //将数据包装成Tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> map1 = filter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of(value.getBehavior(), 1);
            }
        });

        //根据行为进行分组分区
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map1.keyBy(0).sum(1);

        //打印流
        sum.print();

        //提交job
        env.execute();
    }
}
