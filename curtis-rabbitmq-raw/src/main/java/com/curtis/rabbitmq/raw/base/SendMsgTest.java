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
 * @desc RabbitMQ入门示例
 * @date 2021-09-23
 * @email curtis.cai@outlook.com
 * @reference
 */
public class SendMsgTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendMsgTest.class);

    @Test
    public void testSendMsg() {
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
        Connection connection = null;
        try {
            // 获取连接对象
            connection = connectionFactory.newConnection();
            Assert.assertNotNull(connection);
            // 创建连接中的通道
            Channel channel = connection.createChannel();
            String exchangeName = "exchange.test";
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);
            // 通道绑定对应的消息队列，如果队列不存在自动创建
            String queueName = "queue.test.hello";
            // 参数String queue：指定队列名称，队列不存在则自动创建
            // boolean durable：定义队列是否需要持久化
            // boolean exclusive：定义队列是否要独占（队列被当前连接独占）
            // boolean autoDelete：指定再消费完队列中的消息后是否自动删除队列
            // Map<String, Object> arguments：其他参数
            channel.queueDeclare(queueName, false, false, false, null);
            String routingKey = "routingKey.test.hello";
            channel.queueBind(queueName, exchangeName, routingKey);
            // 发布消息
            byte[] msgBytes = "hello world".getBytes(StandardCharsets.UTF_8);
            channel.basicPublish(exchangeName, routingKey, null, msgBytes);
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
