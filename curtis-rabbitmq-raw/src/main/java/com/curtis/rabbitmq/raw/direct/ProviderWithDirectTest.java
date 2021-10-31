package com.curtis.rabbitmq.raw.direct;

import com.curtis.rabbitmq.raw.utils.RabbitMQUtil;
import com.rabbitmq.client.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-10-25
 * @email curtis.cai@outlook.com
 * @reference
 */
public class ProviderWithDirectTest {

    @Test
    public void testSendMsgWithOutUtil() {
        // 创建连接MQ的连接工厂对象
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 设置连接MQ的主机
        connectionFactory.setHost("node101");
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
            channel.exchangeDeclare("exchange-test", BuiltinExchangeType.DIRECT, true);
            // 通道绑定对应的消息队列，如果队列不存在自动创建
            String queueName = "hello";
            // 参数String queue：指定队列名称，队列不存在则自动创建
            // boolean durable：定义队列是否需要持久化
            // boolean exclusive：定义队列是否要独占（队列被当前连接独占）
            // boolean autoDelete：指定再消费完队列中的消息后是否自动删除队列
            // Map<String, Object> arguments：其他参数
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, "exchange-test", "routingKey-test");
            // 发布消息
            for (int i = 0; i < 10; i++) {
                String msg = "hello world" + i;
                byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
                channel.basicPublish("exchange-test", "routingKey-test", null, msgBytes);
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

        // try {
        //     TimeUnit.SECONDS.sleep(30);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
    }

    @Test
    public void testSendMsg() {
        Connection connection = RabbitMQUtil.getConnection();
        Assert.assertNotNull(connection);
        try {
            String queueName = "hello1";
            String exchangeName = "exchange-test";
            String routingKey = "routingKey-test";

            // 创建连接中的通道
            Channel channel = connection.createChannel();
            channel.exchangeDeclare("exchange-test", BuiltinExchangeType.DIRECT, true);
            // 通道绑定对应的消息队列，如果队列不存在自动创建
            // 参数String queue：指定队列名称，队列不存在则自动创建
            // boolean durable：定义队列是否需要持久化(消息并不会持久化，重启服务将丢失)
            // boolean exclusive：定义队列是否要独占（队列被当前连接独占）
            // boolean autoDelete：指定队列中没有消息并且也没有消费者在监听的情况下是否自动删除队列（消费者消费完队列中的所有消息并且断开连接之后是否自动删除队列）
            // Map<String, Object> arguments：其他参数
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, exchangeName, routingKey);
            // 发布消息
            for (int i = 0; i < 10; i++) {
                String msg = "hello world" + i;
                byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
                channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, msgBytes);
            }

            // RabbitMQUtil.closeChannelAndConnection(channel, connection);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
