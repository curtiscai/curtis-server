package com.curtis.rabbitmq.raw.senior;

import com.google.common.collect.Maps;
import com.rabbitmq.client.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-11-10
 * @email curtis.cai@outlook.com
 * @reference
 */
public class MessageTTLTest {

    @Test
    public void testProduceTTLMsgWithTTLQueue() throws IOException, TimeoutException {
        // 1. 创建连接工厂对象
        // 创建连接MQ的连接工厂对象
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 设置连接MQ的主机
        connectionFactory.setHost("node100");
        // 设置连接MQ的端口
        connectionFactory.setPort(5672);
        // 设置连接的虚拟主机
        connectionFactory.setVirtualHost("/virtual-host-test");
        // 设置访问虚拟主机的用户名和密码
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("000000");

        // 2. 从连接工厂对象中获取连接
        Connection connection = connectionFactory.newConnection();
        Assert.assertNotNull(connection);

        // 3. 从连接中获取通道Channel对象
        Channel channel = connection.createChannel();
        Assert.assertNotNull(channel);

        // 4. 声明交换机和队列，并通过绑定键进行绑定
        String exchangeName = "exchange.test.durable";
        String queueName = "queue.test.durable.ttl";
        String routingKey = "routingKey.test.ttl";

        // 4.1 创建非自动删除、持久化的类型为直连的交换机，MQ重启交换器不会丢失
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);

        // 4.2 声明持久化、非自动删除、排他的队列
        Map<String, Object> args = Maps.newHashMap();
        // 设置队列中消息的过期时间，单位毫秒
        args.put("x-message-ttl", 20 * 1000);
        channel.queueDeclare(queueName, true, false, false, args);

        // 4.3 使用路由键绑定交换机和队列
        channel.queueBind(queueName, exchangeName, routingKey);

        // 5. 发送消息
        // 5.1 发送不持久化的消息(默认消息是不持久化的)
        String msg1 = "this is a non-persistent message";
        channel.basicPublish(exchangeName, routingKey, null, msg1.getBytes(StandardCharsets.UTF_8));
        // 5.2 发送持久化的消息
        String msg2 = "this is a persistent message";
        channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, msg2.getBytes(StandardCharsets.UTF_8));

        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
        connection.close();
    }

    @Test
    public void testProduceTTLMsgWithTTLMessage() throws IOException, TimeoutException {
        // 1. 创建连接工厂对象
        // 创建连接MQ的连接工厂对象
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 设置连接MQ的主机
        connectionFactory.setHost("node100");
        // 设置连接MQ的端口
        connectionFactory.setPort(5672);
        // 设置连接的虚拟主机
        connectionFactory.setVirtualHost("/virtual-host-test");
        // 设置访问虚拟主机的用户名和密码
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("000000");

        // 2. 从连接工厂对象中获取连接
        Connection connection = connectionFactory.newConnection();
        Assert.assertNotNull(connection);

        // 3. 从连接中获取通道Channel对象
        Channel channel = connection.createChannel();
        Assert.assertNotNull(channel);

        // 4. 声明交换机和队列，并通过绑定键进行绑定
        String exchangeName = "exchange.test.durable";
        String queueName = "queue.test.durable";
        String routingKey = "routingKey.test";

        // 4.1 创建非自动删除、持久化的类型为直连的交换机，MQ重启交换器不会丢失
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);

        // 4.2 声明持久化、非自动删除、排他的队列
        channel.queueDeclare(queueName, true, false, false, null);

        // 4.3 使用路由键绑定交换机和队列
        channel.queueBind(queueName, exchangeName, routingKey);

        // 5. 发送消息
        // 5.1 发送不持久化的消息(默认消息是不持久化的)
        String msg1 = "this is a non-persistent message";
        channel.basicPublish(exchangeName, routingKey, null, msg1.getBytes(StandardCharsets.UTF_8));
        // 5.2 发送持久化的消息
        String msg2 = "this is a persistent message";
        channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, msg2.getBytes(StandardCharsets.UTF_8));

        // 5.3 发送指定属性的消息(deliveryMode(投递模式):2,持久化消息;priority(优先级):0;expiration(超期时间)单位毫秒;)
        String msg3 = "this is a persistent message with expiration";
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .contentEncoding("UTF-8")
                .deliveryMode(2)
                .priority(0)
                .expiration("20000")
                .messageId("1000001")
                .build();
        channel.basicPublish(exchangeName, routingKey, basicProperties, msg3.getBytes(StandardCharsets.UTF_8));

        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
        connection.close();
    }
}
