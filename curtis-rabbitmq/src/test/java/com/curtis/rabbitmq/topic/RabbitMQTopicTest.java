package com.curtis.rabbitmq.topic;

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
public class RabbitMQTopicTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQTopicTest.class);

    @Autowired
    private TopicProducer topicProducer;

    @Test
    public void testSender(){
        LOGGER.info("ready to send message");
        for (int i = 0; i < 10; i++) {
            topicProducer.sendInfo("hello world to info!");
            topicProducer.sendError("hello world to error!");
        }
        try {
            TimeUnit.SECONDS.sleep(600);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
