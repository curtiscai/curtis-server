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
 * @desc RabbitMQ连接测试
 * @date 2021-09-23
 * @email curtis.cai@outlook.com
 * @reference
 */
public class RabbitMQConnectTest {

    /**
     * 创建连接RabbitMQ连接方式1：属性赋值的方式
     */
    @Test
    public void testConnection1() {
        // 1. 创建连接工厂对象
        // 创建连接MQ的连接工厂对象
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 设置连接MQ的主机
        connectionFactory.setHost("node100");
        // 设置连接MQ的端口
        connectionFactory.setPort(5672);
        // 设置连接的虚拟主机
        connectionFactory.setVirtualHost("/");
        // 设置访问虚拟主机的用户名和密码
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("000000");
        try {
            // 2. 从连接工厂对象中获取连接
            Connection connection = connectionFactory.newConnection();
            Assert.assertNotNull(connection);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建连接RabbitMQ连接方式2：使用连接串的方式
     */
    @Test
    public void testConnection2() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        try {
            connectionFactory.setUri("amqp://admin:000000@node100:5672");
            connectionFactory.setVirtualHost("/virtual-host-test");
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
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
