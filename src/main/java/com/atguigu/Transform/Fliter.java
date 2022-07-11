package com.atguigu.Transform;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:Fliter
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 7:18
 */


public class Fliter {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //TODO 将sensor_id =sensor1 的数据过滤掉
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return new Sensor(s1[0], Long.parseLong(s1[1]), Double.parseDouble(s1[2]));
            }
        });

        SingleOutputStreamOperator<Sensor> filter = map.filter(new FilterFunction<Sensor>() {
            @Override
            public boolean filter(Sensor sensor) throws Exception {
                boolean bool = sensor.getId().equals("sensor1");
                return !bool;
            }
        });

        //打印流
        filter.print();

        //提交job

        env.execute();

    }
}
