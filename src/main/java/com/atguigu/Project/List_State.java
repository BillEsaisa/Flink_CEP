package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.lang.model.util.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:List_State
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/9 18:13
 *
 * 针对每个传感器输出最高的3个水位值
 */


public class List_State {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //转换数据格式、
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));

            }
        });

        //按照传感器id进行分组
        KeyedStream<Sensor, Tuple> keyedStream = map.keyBy("id");

        //进行数据处理
        keyedStream.process(new KeyedProcessFunction<Tuple, Sensor, List<Double>>() {
            //定义状态
            private ListState<Double> Top3State;
            @Override
            public void processElement(Sensor value, KeyedProcessFunction<Tuple, Sensor,  List<Double>>.Context ctx, Collector< List<Double>> out) throws Exception {
                Top3State.add(value.getValue());
                ArrayList<Double> listtop3 = new ArrayList<>();
                //取出状态值
                for (Double v : Top3State.get()) {
                    listtop3.add(v);

                }
                //排序
                listtop3.sort(new Comparator<Double>() {
                    @Override
                    public int compare(Double o1, Double o2) {
                        return (int) (o2-o1);
                    }
                });

                //如果取出的状态个数大于三个，把最后一个剔除掉，永远状态中只保存三个最大的值
                if(listtop3.size()>3){
                    listtop3.remove(3);
                }
                Top3State.update(listtop3);

                out.collect(listtop3);




            }

            @Override
            public void open(Configuration parameters) throws Exception {
               //初始化状态
                Top3State=getRuntimeContext().getListState(new ListStateDescriptor<Double>("Top3", Double.class));
            }

            @Override
            public void close() throws Exception {
                Top3State.clear();
            }
        }).print();

        env.execute();


    }
}
