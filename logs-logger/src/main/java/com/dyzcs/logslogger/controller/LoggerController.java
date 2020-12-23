package com.dyzcs.logslogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Administrator on 2020/12/23.
 */
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("log")
    public String sendLogToKafka(@RequestParam("logString") String logString) {

        // 创建JSON对象
        JSONObject jsonObject = JSON.parseObject(logString);

        // 添加时间戳
        jsonObject.put("ts", System.currentTimeMillis());

        // 打印到控制台及日志
        log.info(jsonObject.toJSONString());

        // 根据数据中的"type"字段选择发送到不同的字段
        if ("startup".equals(jsonObject.getString("type"))) {
//            kafkaTemplate.send(Lo, jsonObject.toString());
        } else {
//            kafkaTemplate.send(MallConstant.MALL_EVENT, jsonObject.toString());
        }

        return "success";
    }
}
