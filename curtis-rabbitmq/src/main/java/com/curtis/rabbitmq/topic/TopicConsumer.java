package com.curtis.rabbitmq.topic;

import com.curtis.rabbitmq.direct.DirectConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
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
public class TopicConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicConsumer.class);

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.topic.info}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.topic}", type = ExchangeTypes.TOPIC),
                    key = "queue.topic.log.info.*")})
    public void processInfo(String msg) {
        LOGGER.info("receive a msg of info -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.topic.error}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.topic}", type = ExchangeTypes.TOPIC),
                    key = "queue.topic.log.error.*")})
    public void processError(String msg) {
        LOGGER.info("receive a msg of error -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.topic.all}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.topic}", type = ExchangeTypes.TOPIC),
                    key = "queue.topic.log.*.*")})
    public void processAll(String msg) {
        LOGGER.info("receive a msg of all -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
