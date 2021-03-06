package com.curtis.rabbitmq.basic;

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
public class RabbitMQProducer {

    @Autowired
    private AmqpTemplate amqpTemplate;

    @Value("${mq.queue.name}")
    private String queueName;


    public void send(String msg) {
        amqpTemplate.convertAndSend(queueName, msg);
    }

}
