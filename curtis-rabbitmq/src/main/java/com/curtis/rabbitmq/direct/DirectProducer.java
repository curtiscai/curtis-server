package com.curtis.rabbitmq.direct;

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
public class DirectProducer {

    @Autowired
    private AmqpTemplate amqpTemplate;

    @Value("${mq.config.exchange.direct}")
    private String exchangeName;

    @Value("${mq.config.queue.direct.info.routing-key}")
    private String infoRoutingKey;

    @Value("${mq.config.queue.direct.error.routing-key}")
    private String errorRoutingKey;

    public void sendInfo(String msg) {
        amqpTemplate.convertAndSend(exchangeName, infoRoutingKey, msg);
    }

    public void sendError(String msg) {
        amqpTemplate.convertAndSend(exchangeName, errorRoutingKey, msg);
    }
}
