/**
 * Copyright (C) 2017 Scott Feldstein
 *
 * Permission is hereby granted, free of charge, to any person obtaining a 
 * copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.scottieknows.kafka;

import static java.lang.String.*;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MyKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(MyKafkaProducer.class);
    
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @Scheduled(fixedRate=10000, initialDelay=30000)
    public void push() {
        try {
            String body = format("%s message generated at %s", UUID.randomUUID(), getDateBuf());
            logger.info("sending {} to {}", body, kafkaConfiguration.getBrokers());
            pushMsg(body);
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
        }
    }

    public void pushMsg(String body) {
        String uri = format("kafka:test?brokers=%s", kafkaConfiguration.getBrokers());
        producerTemplate.sendBody(uri, body);
    }

    private static String getDateBuf() {
        return DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM).format(new Date(System.currentTimeMillis()));
    }

}
