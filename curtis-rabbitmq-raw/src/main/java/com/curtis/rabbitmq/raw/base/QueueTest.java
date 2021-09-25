package com.curtis.rabbitmq.raw.base;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-09-23
 * @email curtis.cai@outlook.com
 * @reference
 */
public class QueueTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueTest.class);

    @Test
    public void testQueue() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("node100");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("000000");

        String exchangeName = "exchange-test";
        String queueName = "queue-test";
        String routingKey = "routingKey-info";
        try {
            Connection connection = connectionFactory.newConnection();
            Assert.assertNotNull(connection);

            Channel channel = connection.createChannel();
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, exchangeName, routingKey);

            LocalTime startTime = LocalTime.now();
            for (int i = 0; i < 10000; i++) {
                String msg = "hello world " + i;
                byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
                channel.basicPublish(exchangeName, routingKey, null, msgBytes);
            }
            LocalTime endTime = LocalTime.now();
            LOGGER.info("spend time -> {}", Duration.between(startTime, endTime));
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}
