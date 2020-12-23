package com.dyzcs.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {
    // 读取配置文件
    private val properties: Properties = PropertiesUtil.load("config.properties")

    // kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.get("kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> properties.get("kafka.group.id"),
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // 读取kafka数据创建流
    def getKafkaStream(ssc: StreamingContext, topics: Set[String]): InputDStream[ConsumerRecord[String, String]] = {
        // 读取kafka数据创建流
        val kafkaDStream = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaPara))
        // 返回
        kafkaDStream
    }
}
