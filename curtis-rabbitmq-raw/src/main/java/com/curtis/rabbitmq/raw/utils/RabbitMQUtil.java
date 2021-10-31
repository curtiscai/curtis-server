package com.curtis.rabbitmq.raw.utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-10-25
 * @email curtis.cai@outlook.com
 * @reference
 */
public class RabbitMQUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQUtil.class);

    private static ConnectionFactory connectionFactory = new ConnectionFactory();

    static {
        // 设置连接MQ的主机
        connectionFactory.setHost("node101");
        // 设置连接MQ的端口
        connectionFactory.setPort(5672);
        // 设置连接的虚拟主机
        connectionFactory.setVirtualHost("/");
        // 设置访问虚拟主机的用户名和密码
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("000000");
    }

    /**
     * 创建RabbitMQ连接
     *
     * @return
     */
    public static Connection getConnection() {
        try {
            return connectionFactory.newConnection();
        } catch (IOException | TimeoutException e) {
            // e.printStackTrace();
            LOGGER.error(e.getClass().getName(), e);
        }
        return null;
    }

    public static void closeChannelAndConnection(Channel channel, Connection connection) {
        try {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException | TimeoutException e) {
            // e.printStackTrace();
            LOGGER.error(e.getClass().getName(), e);
        }
    }
}
