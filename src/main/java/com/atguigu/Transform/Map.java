package com.atguigu.Transform;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:Map
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 6:43
 */


public class Map {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //TODO 将获取到的数据转换成Sensor类
        SingleOutputStreamOperator<Sensor> mapStream = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //打印流
        mapStream.print();

        //提交Job
        env.execute();

    }
}
