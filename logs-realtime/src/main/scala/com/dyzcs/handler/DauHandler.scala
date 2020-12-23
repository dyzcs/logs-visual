package com.dyzcs.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.dyzcs.bean.StartupLog
import com.dyzcs.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

object DauHandler {

    private val sdf = new SimpleDateFormat("yyyy-MM-dd")

    /**
     * 将两次过滤后的数据集中的Mid写入Redis
     *
     * @param filterByMidGroupLogDStream 两次过滤后的数据集
     */
    def saveMidToRedis(filterByMidGroupLogDStream: DStream[StartupLog]): Unit = {
        // 将数据转换为RDD进行操作
        filterByMidGroupLogDStream.foreachRDD(rdd => {
            // 对每个分区单独写库
            rdd.foreachPartition(iter => {
                // a.获取Redis连接
                val jedisClient = RedisUtil.getJedisClient

                // b.写库操作
                iter.foreach(log => jedisClient.sadd(s"dau:${log.logDate}", log.mid))

                // c.释放连接
                jedisClient.close()
            })
        })
    }

    /**
     * 对经过Redis去重之后的数据集，按照Mid分组进行去重
     *
     * @param filterByRedisLogDStream redis过滤后数据
     * @return 过滤后数据集
     */
    def filterByMidGroup(filterByRedisLogDStream: DStream[StartupLog]): DStream[StartupLog] = {
        // 转换数据结构 log => (mid, log)
        val midToLogDStream = filterByRedisLogDStream.map(log => (log.mid, log))

        // 按照mid分组
        val midToLogIterDStream = midToLogDStream.groupByKey()

        // 组内取时间第一条
        val filterByMidGroupLogDStream = midToLogIterDStream.flatMapValues(iter => {
            // 按照时间戳排序并取第一条
            iter.toList.sortWith(_.ts < _.ts).take(1)
        })

        // 返回值
        filterByMidGroupLogDStream.map(_._2)
    }

    /**
     * 根据Redis中数据进行去重
     *
     * @param startupLogDStream 原始数据集
     * @return 过滤后数据集
     */
    def filterByRedis(startupLogDStream: DStream[StartupLog], sc: SparkContext): DStream[StartupLog] = {
        //        // 方案一: 将数据转换为RDD
        //        val filterByRedisLogDStream = startupLogDStream.transform(rdd => {
        //            // 对RDD的每个分区单独处理，减少连接的创建
        //            val filterRDD = rdd.mapPartitions(iter => {
        //                // a.获取redis连接
        //                val jedisClient = RedisUtil.getJedisClient
        //                // b.过滤
        //                val logs = iter.filter(log => !jedisClient.sismember(s"dau:${log.logDate}", log.mid))
        //                // c.关闭
        //                jedisClient.close()
        //                // d.返回过滤后的数据
        //                logs
        //            })
        //            // 返回过滤后的数据
        //            filterRDD
        //        })

        // 方案二: 每个批次获取用户信息，广播至Executor
        val filterByRedisLogDStream = startupLogDStream.transform(rdd => {
            // Driver端，每个批次执行一次
            // a.获取当前时间
            val ts: Long = System.currentTimeMillis()
            val date = sdf.format(new Date(ts))
            // b.获取连接
            val jedisClient = RedisUtil.getJedisClient
            // c.查询用户信息
            val uids = jedisClient.smembers(s"dau:$date")
            val uidBC: Broadcast[util.Set[String]] = sc.broadcast(uids)
            // d.释放连接
            jedisClient.close()

            // 过滤
            rdd.filter(log => !uidBC.value.contains(log.mid))
        })

        //        // 方案三: 按照每天的数据独立过滤
        //        val dateToLogs: DStream[(String, Iterable[StartupLog])] =
        //            startupLogDStream.map(log => (log.logDate, log)).groupByKey()
        //
        //        dateToLogs.transform(rdd => {
        //            rdd.map { case (date, logs) =>
        //                // 获取用户信息
        //                // a.获取redis连接
        //                val jedisClient = RedisUtil.getJedisClient
        //                // b.查询用户信息
        //                val uids = jedisClient.smembers(s"dau:$date")
        //                // c.释放连接
        //                jedisClient.close()
        //
        //                logs.filter(log => !uids.contains(log.mid))
        //            }
        //        })

        // 当前根据redis过滤方法的返回值
        filterByRedisLogDStream
    }

}
