package com.curtis.rabbitmq.fanout;

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
public class FanoutConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FanoutConsumer.class);

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.fanout.sms}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.fanout}", type = ExchangeTypes.FANOUT))})
    public void processInfo(String msg) {
        LOGGER.info("receive a msg of sms -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @RabbitListener(bindings = {
            @QueueBinding(value = @Queue(value = "${mq.config.queue.fanout.email}", autoDelete = "true"),
                    exchange = @Exchange(value = "${mq.config.exchange.fanout}", type = ExchangeTypes.FANOUT))})
    public void processError(String msg) {
        LOGGER.info("receive a msg of email -> {}", msg);
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
