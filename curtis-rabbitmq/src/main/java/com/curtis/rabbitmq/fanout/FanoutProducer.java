package com.curtis.rabbitmq.fanout;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-10-31
 * @email curtis.cai@outlook.com
 * @reference
 */
@Component
public class FanoutProducer {

    @Autowired
    private AmqpTemplate amqpTemplate;

    @Value("${mq.config.exchange.fanout}")
    private String exchangeName;

    public void send(String msg) {
        amqpTemplate.convertAndSend(exchangeName,"", msg);
    }
}
