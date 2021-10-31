package com.curtis.rabbitmq.raw.fanout;

import com.curtis.rabbitmq.raw.utils.RabbitMQUtil;
import com.rabbitmq.client.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-10-25
 * @email curtis.cai@outlook.com
 * @reference
 */
public class CustomerWithFanout2Test {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerWithFanout2Test.class);

    @Test
    public void testConsumeMsg2() {
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
            // boolean durable：定义队列是否需要持久化
            // boolean exclusive：定义队列是否要独占（队列被当前连接独占）
            // boolean autoDelete：指定再消费完队列中的消息后是否自动删除队列
            // Map<String, Object> arguments：其他参数
            channel.queueDeclare(queueName2, true, false, false, null);
            channel.queueBind(queueName2, exchangeName, routingKey);
            // 发布消息
            // for (int i = 0; i < 10; i++) {
            //     String msg = "hello world" + i;
            //     byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
            //     channel.basicPublish("exchange-test", "routingKey-test", null, msgBytes);
            // }

            // 消费消息
            // String queue：指定队列名称
            // boolean autoAck：指定是否自动确认，表示消费者自动向RabbitMQ确认消息消费，消费者不关心消息是否正确处理或者是否已经处理完成，而且拿到啊消息后就应答，RabbitMQ将删除消息
            // 这种情况会导致一旦服务宕机，未消费的消息将丢失，所以实际工作中是不会开启自动应答的。
            // Consumer callback：指定消费消息后的回调接口

            channel.basicQos(1); // 每次消费一个
            channel.basicConsume(queueName2, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    LOGGER.info("接收到消息：" + new String(body, StandardCharsets.UTF_8));
                    // super.handleDelivery(consumerTag, envelope, properties, body);
                    try {
                        TimeUnit.SECONDS.sleep(3);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // long deliveryTag：手动确认消息标识
                    // boolean multiple：每次确认一个
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
