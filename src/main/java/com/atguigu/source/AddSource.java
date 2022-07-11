package com.atguigu.source;

import com.atguigu.Bean.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @PackageName:com.atguigu.source
 * @ClassNmae:AddSource
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 5:36
 */


public class AddSource {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 从自定义数据源读取数据

        DataStreamSource<Sensor> addSource = env.addSource(new Mysource());

        //打印流
        addSource.print();

        //提交任务
        env.execute();


    }
    public static class Mysource implements ParallelSourceFunction<Sensor>{
          private Random random=new Random();
          private Boolean isrunning=true;
        @Override
        public void run(SourceContext sourceContext) throws Exception {
         while (isrunning){
             sourceContext.collect(new Sensor("Sensor"+random.nextInt(10000),random.nextLong(),random.nextDouble()*100));
             Thread.sleep(200);
         }

        }

        @Override
        public void cancel() {
            isrunning=false;

        }
    }

}
