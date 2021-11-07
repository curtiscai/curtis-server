package com.curtis.rabbitmq.raw.base;

import com.rabbitmq.client.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc 队列声明测试
 * @date 2021-11-05
 * @email curtis.cai@outlook.com
 * @reference
 */
public class DeclareQueueTest {

    @Test
    public void testDeclareQueue() throws IOException, TimeoutException {
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

        // 4. 声明队列
        // durable 是否进行持久化，如果进行持久化服务器重启后队列不会丢失，否则不进行持久化重启后队列及队列中的信息将丢失
        // autoDelete 是否进行自动删除，删除的前提是至少有一个消费者连接到这个队列，之后所有与这个队列连接的消费者都断开后，才会自动删除，
        // 注意不能错误地将这个参数理解为"当连接到此队列的所有客户端断开时，这个队列自动删除"，因为如果没有消费者客户端与这个队列连接时，都不会自动删除该队列。

        // 声明自动删除的、非持久化的、排他的队列
        // Actively declare a server-named exclusive, autodelete, non-durable queue.
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare();

        // 声明持久化、非自动删除的队列
        channel.queueDeclare("queue.test.durable", true, true, false, null);
        // 声明持久化、自动删除的队列
        channel.queueDeclare("queue.test.durable.autoDelete", true, true, true, null);
        // 声明非持久化、自动删除的队列
        channel.queueDeclare("queue.test.non-durable.autoDelete", false, true, true, null);

        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
