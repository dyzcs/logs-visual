package com.dyzcs.app

import com.alibaba.fastjson.JSON
import com.dyzcs.bean.EventLog
import com.dyzcs.constants.LogsConstant
import com.dyzcs.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by Administrator on 2020/12/24.
 */
object AlertApp {
    def main(args: Array[String]): Unit = {
        // 1.创建SparkConf
        val sparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

        // 2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 创建时间转换对象
        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        // 3.读取Kafka数据转换流
        val kafkaDStream = MyKafkaUtil.getKafkaStream(ssc, Set(LogsConstant.LOGS_EVENT))

        // 4.将每一条数据转换为样例类对象
        val eventLogDStream = kafkaDStream.map(record => {
            // a.将record转换为样例类对象
            val eventLog = JSON.parseObject(record.value(), classOf[EventLog])

            // b.处理日期和时间
            val dateHourStr = sdf.format(new Date(eventLog.ts))
            //            println(dateHourStr)
            val dateHourArr = dateHourStr.split("-")
            //            println(dateHourArr(0))
            //            println(dateHourArr(1))
            eventLog.logDate = dateHourArr(0)
            eventLog.logHour = dateHourArr(1)

            // c.返回结果
            eventLog
        })

        // 5.开窗
        val eventLogWindowDStream = eventLogDStream.window(Seconds(30))

        // 6.转换数据结构并按照mid分组
        val midToLogIterDStream: DStream[(String, Iterable[EventLog])] =
            eventLogWindowDStream.map(log => (log.mid, log)).groupByKey()
        

        // 9.启动任务
        ssc.start()
        ssc.awaitTermination()
    }
}
