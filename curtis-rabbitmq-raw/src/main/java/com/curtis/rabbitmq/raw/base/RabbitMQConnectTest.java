package com.curtis.rabbitmq.raw.base;

import com.rabbitmq.client.*;
import org.checkerframework.checker.units.qual.C;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc RabbitMQ链接测试
 * @date 2021-09-23
 * @email curtis.cai@outlook.com
 * @reference
 */
public class RabbitMQConnectTest {

    @Test
    public void testConnection1() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("node101");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("000000");
        try {
            Connection connection = connectionFactory.newConnection();
            Assert.assertNotNull(connection);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        try {
            TimeUnit.SECONDS.sleep(30);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConnection2() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        try {
            connectionFactory.setUri("amqp://admin:000000@node101:5672");
            connectionFactory.setVirtualHost("/");
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
        }

        try {
            Connection connection = connectionFactory.newConnection();
            Assert.assertNotNull(connection);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        try {
            TimeUnit.SECONDS.sleep(30);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
