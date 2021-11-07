package com.curtis.rabbitmq.raw.base;

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
 * @date 2021-11-07
 * @email curtis.cai@outlook.com
 * @reference
 */
public class BasicPublishTest {

    @Test
    public void testBasicPublish() throws IOException, TimeoutException {
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
        String exchangeName = "exchange.test.direct";
        String queueName = "queue.test.durable";
        String routingKey = "routingKey.test";

        // 4.1 创建非自动删除、持久化的类型为直连的交换机，MQ重启交换器不会丢失
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);

        // 4.2 声明持久化、非自动删除、排他的队列
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(queueName, true, true, false, null);
        System.out.println(declareOk);


        // 4.3 使用路由键绑定交换机和队列
        channel.queueBind(queueName, exchangeName, routingKey);


        for (int i = 1; i <= 10; i++) {
            String msg = "this is the message of ";
            byte[] msgBytes = (msg + i).getBytes(StandardCharsets.UTF_8);
            channel.basicPublish(exchangeName, routingKey, null, msgBytes);
        }

        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        channel.close();
        connection.close();
    }
}
