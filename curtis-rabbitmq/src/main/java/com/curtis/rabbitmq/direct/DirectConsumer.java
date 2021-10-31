package com.curtis.rabbitmq.direct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
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
public class DirectConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectConsumer.class);

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.direct.info}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.direct}", type = ExchangeTypes.DIRECT),
                    key = "${mq.config.queue.direct.info.routing-key}")})
    public void processInfo(String msg) {
        LOGGER.info("receive a msg of info -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.direct.info}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.direct}", type = ExchangeTypes.DIRECT),
                    key = "${mq.config.queue.direct.info.routing-key}")})
    public void processInfo2(String msg) {
        LOGGER.info("receive a msg of info 2 -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.direct.error}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.direct}", type = ExchangeTypes.DIRECT),
                    key = "${mq.config.queue.direct.error.routing-key}")})
    public void processError(String msg) {
        LOGGER.info("receive a msg of error -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
