package com.atguigu.Project;

import com.atguigu.Bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:Ads_Click
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 22:23
 * 各省份页面广告点击量实时统计
 *
 */


public class Ads_Click {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取日志数据
        DataStreamSource<String> readTextFile = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\AdClickLog.csv");

        //包装数据——javaBean
        SingleOutputStreamOperator<AdsClickLog> map = readTextFile.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");

                return new AdsClickLog(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2], split[3], Long.parseLong(split[4]));
            }
        });

        //包装成Tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> map1 = map.map(new MapFunction<AdsClickLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(AdsClickLog value) throws Exception {
                return Tuple2.of(value.getProvince()+"-"+value.getAdId(), 1);
            }
        });

        //根据Tuple f0进行keyby
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map1.keyBy(0).sum(1);

        //打印输出
        sum.print();

        //提交job
        env.execute();
    }
}
