package com.curtis.rabbitmq.raw.base;

import com.rabbitmq.client.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc 交换机声明测试
 * @date 2021-11-05
 * @email curtis.cai@outlook.com
 * @reference
 */
public class DeclareExchangeTest {

    @Test
    public void testDeclareExchange() throws IOException, TimeoutException {
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

        // 4. 声明交换机
        // 创建非自动删除、非持久化的交换机（直连、主题、广播），MQ重启交换机丢失
        // Actively declare a non-autodelete, non-durable exchange with no extra arguments
        AMQP.Exchange.DeclareOk declareDirectNonDurableOk = channel.exchangeDeclare("exchange.test.direct.non-durable", BuiltinExchangeType.DIRECT);
        AMQP.Exchange.DeclareOk declareTopicNonDurableOk = channel.exchangeDeclare("exchange.test.topic.non-durable", BuiltinExchangeType.TOPIC);
        AMQP.Exchange.DeclareOk declareFanoutNonDurableOk = channel.exchangeDeclare("exchange.test.fanout.non-durable", BuiltinExchangeType.FANOUT);

        // 创建非自动删除、持久化的交换机（直连、主题、广播），MQ重启交换器不会丢失
        // Actively declare a non-autodelete exchange with no extra arguments
        channel.exchangeDeclare("exchange.test.direct", BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare("exchange.test.topic", BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare("exchange.test.fanout", BuiltinExchangeType.FANOUT, true);

        // 创建自动删除、非持久化的交换机（直连、主题、广播），。自动
        // 删除的前提是至少有一个队列或者交换器与这个交换器绑定 之后所有与这个交换器绑定的队列或者交换器都与此解绑。
        // 注意不能错误地将这个参数理解为"当与此交换器连接的客户端都断开时，RabbitMQ会自动删除本交换器。
        // Actively declare a non-autodelete exchange with no extra arguments
        channel.exchangeDeclare("exchange.test.direct.non-durable.autodelete", BuiltinExchangeType.DIRECT, false, true, null);
        channel.exchangeDeclare("exchange.test.topic.non-durable.autodelete", BuiltinExchangeType.TOPIC, false, true, null);
        channel.exchangeDeclare("exchange.test.fanout.non-durable.autodelete", BuiltinExchangeType.FANOUT, false, true, null);

        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
