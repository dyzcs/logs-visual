package com.dyzcs.app

import com.alibaba.fastjson.JSON
import com.dyzcs.bean.StartupLog
import com.dyzcs.constants.LogsConstant
import com.dyzcs.handler.DauHandler
import com.dyzcs.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date


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
        //        filterByMidGroupLogDStream.print()

        // 8.gs_dau where logdate=2020-12-24
        // select LOGHOUR lh, count(*) ct fro将数据写入MySQL
        //        // select count(*) from lom logs_dau where LOGDATE='2020-12-24' group by LOGHOUR;
        filterByMidGroupLogDStream.foreachRDD(rdd => {
            rdd.foreachPartition(pars => {
                val connection = DriverManager.getConnection("jdbc:mysql://ip/logsdata?useSSL=false", "root", "")
                val statement1 = connection.prepareStatement("insert into logs_dau values(?,?,?,?,?,?,?,?,?,?,?)")
                while (pars.hasNext) {
                    val log = pars.next()
                    statement1.setString(1, log.mid)
                    statement1.setString(2, log.uid)
                    statement1.setString(3, log.appid)
                    statement1.setString(4, log.area)
                    statement1.setString(5, log.os)
                    statement1.setString(6, log.ch)
                    statement1.setString(7, log.`type`)
                    statement1.setString(8, log.vs)
                    statement1.setString(9, log.logDate)
                    statement1.setString(10, log.logHour)
                    statement1.setLong(11, log.ts)
                    statement1.executeUpdate()
                }
            })
        })

        // 启动
        ssc.start()
        ssc.awaitTermination()
    }
}
