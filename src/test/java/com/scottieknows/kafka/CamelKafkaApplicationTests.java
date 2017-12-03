package com.scottieknows.kafka;

import static java.lang.String.format;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import info.batey.kafka.unit.KafkaUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(classes= {CamelKafkaApplicationTests.Config.class, CamelKafkaApplication.class})
@TestPropertySource(locations="classpath:test.properties")
public class CamelKafkaApplicationTests {

    @Autowired
    private CamelRouteConfig camelRouteConfig;
    @Autowired
    private MyKafkaProducer myKafkaProducer;

    @Configuration
    public static class Config {
        @Bean(destroyMethod="shutdown")
        public KafkaUnit kafkaUnit() throws IOException {
            KafkaUnit kafkaUnitServer = new KafkaUnit() {
                @Override
                public void shutdown() {
                    try {
                        super.shutdown();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            };
            kafkaUnitServer.startup();
            String testTopic = "test";
            kafkaUnitServer.createTopic(testTopic);
            return kafkaUnitServer;
        }

        @Bean
        public KafkaConfiguration kafkaConfiguration(KafkaUnit kafkaUnit) {
            KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
            kafkaConfiguration.setBrokers("localhost:" + kafkaUnit.getBrokerPort());
            return kafkaConfiguration;
        }
    }

    @Test
    public void test() throws InterruptedException {
        String msg = "Test Message from Camel Kafka Component Final";
        int numMsgs = 1000;
        sendMsgs(msg, numMsgs);
        for (int i=0; i<50; i++) {
            if (camelRouteConfig.messages().size() >= numMsgs) {
                break;
            }
            Thread.sleep(500);
        }
        assertEquals(numMsgs, camelRouteConfig.messages().size());
        for (int i=0; i<numMsgs; i++) {
            assertTrue(camelRouteConfig.messages().contains(msg + " " + i));
        }
    }

    private void sendMsgs(String msg, int numMessages) {
        for (int i=0; i<numMessages; i++) {
            int msgNum = i;
            String body = msg + " " + msgNum;
//            String uri = format("kafka:test?brokers=%s", kafkaConfiguration.getBrokers());
//            Map<String, Object> headers = new HashMap<>();
//            headers.put(KafkaConstants.PARTITION_KEY, 0);
//            headers.put(KafkaConstants.KEY, "1");
            myKafkaProducer.pushMsg(body);
        }
    }

}
