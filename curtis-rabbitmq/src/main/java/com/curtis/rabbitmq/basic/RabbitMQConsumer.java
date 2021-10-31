package com.curtis.rabbitmq.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-10-31
 * @email curtis.cai@outlook.com
 * @reference
 */
@Component
public class RabbitMQConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConsumer.class);

    @RabbitListener(queues = {"${mq.queue.name}"})
    public void process(String msg) {
        LOGGER.info("receive a msg -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
