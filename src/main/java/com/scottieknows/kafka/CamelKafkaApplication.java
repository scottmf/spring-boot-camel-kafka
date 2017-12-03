package com.scottieknows.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CamelKafkaApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(CamelKafkaApplication.class, args);
    }

}
