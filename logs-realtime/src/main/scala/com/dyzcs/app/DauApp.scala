package com.dyzcs.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.dyzcs.bean.StartupLog
import com.dyzcs.constants.LogsConstant;
import com.dyzcs.handler.DauHandler
import com.dyzcs.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DauApp {
    def main(args: Array[String]): Unit = {
        // 1.创建SparkConf
        val conf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

        // 2.创建StreamingContext
        val ssc = new StreamingContext(conf, Seconds(5))

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

        // 3.读取kafka数据
        val kafkaDStream = MyKafkaUtil.getKafkaStream(ssc, Set(LogsConstant.LOGS_STARTUP))

        // 4.将其转化为样例类对象
        val startupLogDStream = kafkaDStream.map(record => {
            val log = JSON.parseObject(record.value(), classOf[StartupLog])

            // 获取时间戳
            val ts = log.ts
            // 将时间戳转换成日期字符串
            val dateHour = sdf.format(new Date(ts))
            // 按照空格切分
            val dataHourArr = dateHour.split(" ")

            // 给日期和小时字段赋值
            log.logDate = dataHourArr(0)
            log.logHour = dataHourArr(1)

            // 返回
            log
        })
        // 测试
        //        startupLogDStream.print()

        // 5.结合Redis跨批次进行去重
        val filterByRedisLogDStream: DStream[StartupLog] = DauHandler.filterByRedis(startupLogDStream, ssc.sparkContext)

        // 6.使用分组做同批次去重
        val filterByMidGroupLogDStream: DStream[StartupLog] = DauHandler.filterByMidGroup(filterByRedisLogDStream)
        filterByMidGroupLogDStream.cache()

        // 7.将mid写入Redis
        DauHandler.saveMidToRedis(filterByMidGroupLogDStream)

        // 8.将数据明细写入mysql
        //        filterByMidGroupLogDStream.foreachRDD(rdd => {
        //            import org.apache.phoenix.spark._
        //            rdd.saveToPhoenix("MALL_DAU",
        //                Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        //                new Configuration,
        //                Some("s183,s184:2181"))
        //        })
        // 测试
        filterByMidGroupLogDStream.print()

        // 启动
        ssc.start()
        ssc.awaitTermination()
    }
}
