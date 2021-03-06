package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/4 14:05
  */
// 流处理word count
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(8)
//    env.disableOperatorChaining()
    // TODO: 不要忘记导包！ import org.apache.flink.streaming.api.scala._

    // 从外部命令中提取参数，作为socket主机名和端口号
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

    // 接收一个socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)
    // 进行转化处理统计
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0) // TODO: keyBy就是groupBy
      .sum(1)

    // TODO: flink每一个算子都可以单独设置并行度，不会互相影响  一般输出到文件时会设置并行度为1
    // TODO: 如果不设置1,那么打印的时候，会有前缀 1..,2..,3..,4..
    resultDataStream.print().setParallelism(1)

    // 启动任务执行
    env.execute("stream word count")
  }
}
