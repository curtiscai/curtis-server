package com.curtis.rabbitmq.base;

import com.curtis.rabbitmq.basic.RabbitMQProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2021-10-31
 * @email curtis.cai@outlook.com
 * @reference
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitMQBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQBaseTest.class);

    @Autowired
    private RabbitMQProducer rabbitMQProducer;

    @Test
    public void testSender(){
        LOGGER.info("ready to send message");
        for (int i = 0; i < 10; i++) {
            rabbitMQProducer.send("hello world!");
        }
        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
