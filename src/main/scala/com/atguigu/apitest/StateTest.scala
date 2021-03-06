package com.atguigu.apitest

import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/8 11:28
  */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new FsStateBackend("", true))
    env.setStateBackend(new RocksDBStateBackend(""))

    // 开启checkpoint
    env.enableCheckpointing()

    // checkpoint的配置（生产环境一般最多配置前五个即可）
    val chkpConfig = env.getCheckpointConfig
    chkpConfig.setCheckpointInterval(10000L)//前一个checkPoint头部到后一个checkPoint头部的间隔时间
    chkpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    chkpConfig.setCheckpointTimeout(60000)//checkpoint执行时间，超时该checkPoint就会被丢弃
    chkpConfig.setMaxConcurrentCheckpoints(2)//最大并行执行的checkPoint任务个数，默认1
    chkpConfig.setMinPauseBetweenCheckpoints(500L)//前后两次checkPoint最小间隔时间（前一个尾部到后一个头部的间隔时间）
    chkpConfig.setPreferCheckpointForRecovery(true)
    chkpConfig.setTolerableCheckpointFailureNumber(0)

    // 重启策略配置
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L)) //3次重启，失败之间时间间隔10s
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(5), Time.seconds(10)))//在5分钟内失败5次，失败之间间隔时间是10s

    // 读取数据
    //    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    //    val inputStream = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      } )
      .uid("1")

    // 需求：对于温度传感器温度值跳变，超过10度，报警
     val alertStream = dataStream
      .keyBy(_.id)
//      .flatMap( new TempChangeAlert(10.0) ) todo 需要在外部定义一个RichFunction  不推荐!
      .flatMapWithState[(String, Double, Double), Double] { // TODO: 只有keyedStream才有withState方法 ！！
         case (data: SensorReading, None) => ( List.empty, Some(data.temperature) )
         case (data: SensorReading, lastTemp: Some[Double]) => {
           // 跟最新的温度值求差值作比较
           val diff = (data.temperature - lastTemp.get).abs
           if( diff > 10.0 )
             ( List((data.id, lastTemp.get, data.temperature)), Some(data.temperature) )
           else
             ( List.empty, Some(data.temperature) )
         }
     }

    alertStream.print()

    env.execute("state test")
  }
}

// 实现自定义RichFlatmapFunction
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义状态保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val flagState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("flag", classOf[Boolean]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值作比较
    val diff = (value.temperature - lastTemp).abs
    if( !flagState.value() && diff > threshold )
      out.collect( (value.id, lastTemp, value.temperature) )

    // 更新状态
    lastTempState.update(value.temperature)
    flagState.update(true)
  }
}

// Keyed state测试：todo 必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper1 extends RichMapFunction[SensorReading, String]{
  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState( new ListStateDescriptor[Int]("liststate", classOf[Int]) )
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState( new MapStateDescriptor[String, Double]("mapstate", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reducestate", new MyReducer, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState( new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    // 状态的读写
    val myV = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    listState.update(list)
    listState.get()

    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)

    reduceState.get()
    reduceState.add(value)

    value.id
  }
}