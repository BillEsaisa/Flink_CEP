package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * @ClassNmae:Value_State
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/9 17:37
 *
 * 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
 * (连续的两个水位线差值，所以需要拿到上一个的水位线的值)：使用状态编程
 */


public class Value_State {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从socket端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //map转换成pojo
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //按照传感器id进行分组
        KeyedStream<Sensor, Tuple> keyedStream = map.keyBy("id");

        //处理数据
        SingleOutputStreamOperator<String> lastvalue = keyedStream.process(new KeyedProcessFunction<Tuple, Sensor, String>() {
            //定义状态
            private ValueState<Double> lastvalue_state;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                lastvalue_state = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastvalue", Double.class));
            }

            @Override
            public void processElement(Sensor value, KeyedProcessFunction<Tuple, Sensor, String>.Context ctx, Collector<String> out) throws Exception {
                //判空
                if (lastvalue_state.value() == null) {
                    lastvalue_state.update(value.getValue());

                } else if (Math.abs(lastvalue_state.value() - value.getValue()) > 10) {
                    out.collect("连续两个水位线差值超过10，报警");
                    lastvalue_state.update(value.getValue());

                }


            }

            @Override
            public void close() throws Exception {
                lastvalue_state.clear();
            }
        });

        //打印流
        lastvalue.print();

        //提交job
        env.execute();


    }
}
