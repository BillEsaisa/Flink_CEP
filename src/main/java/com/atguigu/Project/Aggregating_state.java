package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:Aggregating_state
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/9 21:07
 *
 * 计算每个传感器的平均水位
 */


public class Aggregating_state {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //将数据格式转换成POJO类型
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));

            }
        });

        //按照传感器id分组
        KeyedStream<Sensor, Tuple> keyedStream = map.keyBy("id");

        //状态编程
        keyedStream.process(new KeyedProcessFunction<Tuple, Sensor, Double>() {
            //定义状态
            private AggregatingState<Double,Double> avgstate;
            @Override
            public void processElement(Sensor value, KeyedProcessFunction<Tuple, Sensor, Double>.Context ctx, Collector<Double> out) throws Exception {
                avgstate.add(value.getValue());
                out.collect(avgstate.get());



            }

            @Override
            public void open(Configuration parameters) throws Exception {
               //初始化状态
                avgstate=getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Double, Tuple2<Double,Integer>, Double>("avg_state", new AggregateFunction<Double, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<Double,Integer>(0.0,0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(Double value, Tuple2<Double, Integer> accumulator) {
                        accumulator.f0+=value;
                        accumulator.f1+=1;
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        double result = accumulator.f0 / accumulator.f1;
                        return result;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        a.f0+= b.f0;
                        a.f1+= b.f1;
                        return a;
                    }
                }, Types.TUPLE(Types.DOUBLE,Types.INT)));
            }

            @Override
            public void close() throws Exception {
                avgstate.clear();

            }
        }).print();

        //提交job
        env.execute();

    }
}
