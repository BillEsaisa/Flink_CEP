package com.atguigu.Transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:Union
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 8:45
 *
 * union:这也是个合流的算子
 * Uion,可以合并多条流，真正的合并，返回值类型是dataStream ,和connect不同的是connect返回的是connect 类型的流，也验证了connect合并的流并没真正的合并在一起
 * 虽然union 能真正的将多条流合并在一起，但是对流中存储的数据类型要求必须是一致的
 *
 */


public class Union {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从网络端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102",7777);

        //从文件中读取数据
        DataStreamSource<String> textFile = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\textsource");

        //TODO 将两条流合流

        DataStream<String> union = socketTextStream.union(textFile);

        //打印流
        union.print();

        //提交job
        env.execute();
    }
}
