package com.curtis.rabbitmq.topic;

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
public class TopicProducer {

    @Autowired
    private AmqpTemplate amqpTemplate;

    @Value("${mq.config.exchange.topic}")
    private String exchangeName;

    public void sendInfo(String msg) {
        amqpTemplate.convertAndSend(exchangeName, "queue.topic.log.info.user", msg);
        amqpTemplate.convertAndSend(exchangeName, "queue.topic.log.info.product", msg);
    }

    public void sendError(String msg) {
        amqpTemplate.convertAndSend(exchangeName, "queue.topic.log.error.user", msg);
        amqpTemplate.convertAndSend(exchangeName, "queue.topic.log.error.product", msg);
    }
}
