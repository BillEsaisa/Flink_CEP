package com.atguigu.Project;

import com.atguigu.Bean.AppLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import scala.App;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:AppAnalysis_ByChannel
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 22:40
 *
 *
 * App渠道推广分析
 */


public class AppAnalysis_ByChannel {
    public static void main(String[] args) throws Exception {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //从自定义source 中读数据（模拟生成数据）
        DataStreamSource<AppLog> addSource = env.addSource(new ParallelSourceFunction<AppLog>() {
            Random random = new Random();
            Boolean isrunning = true;
            List<String> Channel = Arrays.asList("小米", "华为", "苹果", "VIVO", "OppO", "荣耀");
            List<String> bahaves = Arrays.asList("install", "uninstall", "update", "download");


            @Override
            public void run(SourceContext<AppLog> ctx) throws Exception {
                while (isrunning) {
                    ctx.collect(new AppLog((long)random.nextInt(10000),bahaves.get(random.nextInt(bahaves.size())),
                            Channel.get(random.nextInt(bahaves.size())),System.currentTimeMillis()));
                    Thread.sleep(200);

                }


            }

            @Override
            public void cancel() {
                isrunning = false;

            }
        });

        // 将数据包装成Tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = addSource.map(new MapFunction<AppLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(AppLog value) throws Exception {

                return Tuple2.of(value.getChannel() + "-" + value.getBehavior(), 1);
            }
        });

        //按照Tuple fo 进行keyby
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);

        //打印流
        sum.print();

        //提交job
        env.execute();
    }
}
