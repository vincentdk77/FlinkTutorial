package com.atguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/10 16:30
  */
object FileOutputTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 连接外部系统，读取数据，注册表
    val filePath = "D:\\JavaRelation\\Workpaces\\myproject\\bigData\\flink2020\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())//需要引入flink csv依赖
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
//        .field("pt", DataTypes.TIMESTAMP(3)).proctime()
      )
      .createTemporaryTable("inputTable")

    // 3. 转换操作
    val sensorTable = tableEnv.from("inputTable")
    // 3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id)    // 基于id分组
      .select('id, 'id.count as 'count)

    // 4. 输出到文件
    // 注册输出表
    val outputPath = "D:\\JavaRelation\\Workpaces\\myproject\\bigData\\flink2020\\FlinkTutorial\\src\\main\\resources\\output.txt"

    tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
//        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")

    // TODO: CSV TableSink，如果做了更新操作就不能写了，只支持新增数据 ！！所以这里aggTable无法insert
    resultTable.insertInto("outputTable")

    resultTable.toAppendStream[(String, Double)].print("result")

    // TODO: AggTable不能转成追加流，因为会更新数据！用撤回模式retractStream，实际上是加了一个boolean字段，最新更新的数据为true，老的数据为false
//    在撤回模式下，表和外部连接器交换的是：添加（Add）和撤回（Retract）消息。
//     插入（Insert）会被编码为添加消息；
//     删除（Delete）则编码为撤回消息；
//     更新（Update）则会编码为，已更新行（上一行）的撤回消息，和更新行（新行） 的添加消息。
//    在此模式下，不能定义 key，这一点跟 upsert 模式完全不同
    aggTable.toRetractStream[Row].print("agg")

    env.execute()
  }
}
