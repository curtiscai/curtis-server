package com.curtis.rabbitmq.raw.base;

import com.rabbitmq.client.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc 消费消息测试
 * @date 2021-11-08
 * @email curtis.cai@outlook.com
 * @reference
 */
public class RabbitMQConsumeMsgTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConsumeMsgTest.class);

    /**
     * 使用非自动应答、服务器生成消费标签的方式来启动非本地、非独占的消费者
     *
     * @throws IOException
     * @throws TimeoutException
     */
    @Test
    public void testProduceMsgWithNonAutoAck() throws IOException, TimeoutException {
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

        LOGGER.info("The Class Name Of Channel Is：{}", channel.getClass().getName());

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

        // 5. 消费消息
        // 默认需要手动应答，如果不手动应答，连接断开后消息将恢复待消费状态
        // channel.basicQos(1);
        channel.basicConsume(queueName, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                LOGGER.info("deliveryTag -> {}", deliveryTag);

                String msg = new String(body, StandardCharsets.UTF_8);
                LOGGER.info("consume msg -> {}", msg);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 手动应答
                channel.basicAck(deliveryTag, false);
            }
        });

        LOGGER.info("The program is about to complete");
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
        connection.close();
    }

    /**
     * 使用自动应答、服务器生成消费标签的方式来启动非本地、非独占的消费者
     * @throws IOException
     * @throws TimeoutException
     */
    @Test
    public void testProduceMsgWithAutoAck() throws IOException, TimeoutException {
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

        LOGGER.info("The Class Name Of Channel Is：{}", channel.getClass().getName());

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

        // 5. 消费消息
        // 如果指定自动应答
        channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                LOGGER.info("deliveryTag -> {}", deliveryTag);

                String msg = new String(body, StandardCharsets.UTF_8);
                LOGGER.info("consume msg -> {}", msg);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        LOGGER.info("The program is about to complete");
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
        connection.close();
    }


    @Test
    public void testProduceMsgWithNonAutoAckWithNotAck() throws IOException, TimeoutException {
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

        LOGGER.info("The Class Name Of Channel Is：{}", channel.getClass().getName());

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

        // 5. 消费消息
        // 如果指定自动应答
        channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                LOGGER.info("deliveryTag -> {}", deliveryTag);

                String msg = new String(body, StandardCharsets.UTF_8);
                LOGGER.info("consume msg -> {}", msg);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        LOGGER.info("The program is about to complete");
        try {
            TimeUnit.SECONDS.sleep(30);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
        connection.close();
    }

    @Test
    public void testProduceMsgWithAutoAckWithException() throws IOException, TimeoutException {
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

        LOGGER.info("The Class Name Of Channel Is：{}", channel.getClass().getName());

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

        // 5. 消费消息
        // 如果指定自动应答
        channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                LOGGER.info("deliveryTag -> {}", deliveryTag);

                String msg = new String(body, StandardCharsets.UTF_8);
                LOGGER.info("consume msg -> {}", msg);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                throw new RuntimeException("throw an exception");
            }
        });

        LOGGER.info("The program is about to complete");
        try {
            TimeUnit.SECONDS.sleep(30);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
        connection.close();
    }
}
