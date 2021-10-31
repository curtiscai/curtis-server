package com.curtis.rabbitmq.raw.fanout;

import com.curtis.rabbitmq.raw.utils.RabbitMQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-10-25
 * @email curtis.cai@outlook.com
 * @reference
 */
public class ProviderWithFanoutTest {

    @Test
    public void testSendMsg() {
        Connection connection = RabbitMQUtil.getConnection();
        Assert.assertNotNull(connection);
        try {
            String queueName1 = "queue-fanout-test1";
            String queueName2 = "queue-fanout-test2";
            String exchangeName = "exchange-fanout-test";
            String routingKey = "routingKey-fanout-test";

            // 创建连接中的通道
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);
            // 通道绑定对应的消息队列，如果队列不存在自动创建

            // 参数String queue：指定队列名称，队列不存在则自动创建
            // boolean durable：定义队列是否需要持久化(消息并不会持久化，重启服务将丢失)
            // boolean exclusive：定义队列是否要独占（队列被当前连接独占）
            // boolean autoDelete：指定队列中没有消息并且也没有消费者在监听的情况下是否自动删除队列（消费者消费完队列中的所有消息并且断开连接之后是否自动删除队列）
            // Map<String, Object> arguments：其他参数
            // channel.queueDeclare(queueName1, true, false, false, null);
            // channel.queueBind(queueName1, exchangeName, routingKey);
            //
            // channel.queueDeclare(queueName2, true, false, false, null);
            // channel.queueBind(queueName2, exchangeName, routingKey);
            // 发布消息
            for (int i = 1; i <= 100; i++) {
                String msg = "fanout message - " + i;
                byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
                channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, msgBytes);
            }

            // RabbitMQUtil.closeChannelAndConnection(channel, connection);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
