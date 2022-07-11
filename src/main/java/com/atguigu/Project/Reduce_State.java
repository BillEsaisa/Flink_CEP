package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
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
 * @ClassNmae:Reduce_State
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/9 20:29
 *
 * 计算每个传感器的水位和
 */


public class Reduce_State {
    public static void main(String[] args) throws Exception {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //将数据转换成pojo
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //按照传感器id 分组
        KeyedStream<Sensor, Tuple> keyedStream = map.keyBy("id");

        //状态编程
        keyedStream.process(new KeyedProcessFunction<Tuple, Sensor, Double>() {
            //定义状态
            private ReducingState<Double> sumstate;
            @Override
            public void processElement(Sensor value, KeyedProcessFunction<Tuple, Sensor, Double>.Context ctx, Collector<Double> out) throws Exception {
                sumstate.add(value.getValue());
                out.collect(sumstate.get());


            }

            @Override
            public void open(Configuration parameters) throws Exception {
               //初始化状态
                sumstate=getRuntimeContext().getReducingState(new ReducingStateDescriptor<Double>("last_sum_state", new ReduceFunction<Double>() {
                    @Override
                    public Double reduce(Double value1, Double value2) throws Exception {
                        return value1+value2;
                    }
                },Double.class));
            }

            @Override
            public void close() throws Exception {
                sumstate.clear();
            }
        }).print();

        //提交job
        env.execute();

    }
}
