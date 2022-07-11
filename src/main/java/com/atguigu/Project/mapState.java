package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:MapState
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/9 21:37
 *
 * 去掉同一个传感器中重复的水位值
 *
 */


public class mapState {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //将数据类型转换成POJO
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //按照传感器id进行分组
        KeyedStream<Sensor, Tuple> keyedStream = map.keyBy("id");

        //状态编程
        keyedStream.process(new KeyedProcessFunction<Tuple, Sensor, Sensor>() {
            //定义状态
            private MapState<Double,Sensor> map_state;
            @Override
            public void processElement(Sensor value, KeyedProcessFunction<Tuple, Sensor, Sensor>.Context ctx, Collector<Sensor> out) throws Exception {
                if (!map_state.contains(value.getValue())){
                    map_state.put(value.getValue(), value);
                    out.collect(value);

                }

            }

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                map_state=getRuntimeContext().getMapState(new MapStateDescriptor<Double, Sensor>("map_state",Double.class,Sensor.class));
            }

            @Override
            public void close() throws Exception {
                map_state.clear();
            }
        }).print();


        //提交job
        env.execute();
    }
}
