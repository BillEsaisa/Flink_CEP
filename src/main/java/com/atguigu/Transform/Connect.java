package com.atguigu.Transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:Connect
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 8:31
 *
 * connect:合流操作，只能合两条流，并且这两条流connect之后并不是真正意义上的一条流，在内部依旧保持着各自的数据与形式
 *
 *这两条流完全是自己玩自己的
 *
 *
 */


public class Connect {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从网络端口读取数据流、
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //从文件中读取流
        DataStreamSource<String> textFile = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\textsource");

        //TODO 将两条流合流
        ConnectedStreams<String, String> connect = socketTextStream.connect(textFile);

        //打印流
        connect.getFirstInput().print("first");
        connect.getSecondInput().print("second");

        //提交job
        env.execute();
    }

}
