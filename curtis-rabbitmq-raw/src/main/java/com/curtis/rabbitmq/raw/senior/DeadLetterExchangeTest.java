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
public class DeadLetterExchangeTest {

    @Test
    public void testDeadLetterExchange() throws IOException, TimeoutException {
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
        String dlxExchangeName = "dlx.exchange.test.durable";
        String dlxQueueName = "dlx.queue.test.durable.ttl";

        String exchangeName = "exchange.test.durable";
        String queueName = "queue.test.durable.ttl";

        String dlxRoutingKey = "dlx.routingKey.test.ttl";
        String routingKey = "routingKey.test.ttl";

        // 4.1 创建非自动删除、持久化的类型为直连的交换机，MQ重启交换器不会丢失
        channel.exchangeDeclare(dlxExchangeName, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(dlxQueueName, true, false, false, null);
        channel.queueBind(dlxQueueName, dlxExchangeName, dlxRoutingKey);


        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);
        Map<String, Object> args = Maps.newHashMap();
        // 设置队列中消息的过期时间，单位毫秒
        args.put("x-message-ttl", 20 * 1000);
        args.put("x-dead-letter-exchange", dlxExchangeName);
        args.put("x-dead-letter-routing-key", dlxRoutingKey);
        channel.queueDeclare(queueName, true, false, false, args);
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
}
