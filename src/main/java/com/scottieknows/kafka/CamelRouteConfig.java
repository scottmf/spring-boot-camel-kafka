/**
 * Copyright (C) 2015 Scott Feldstein
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CamelRouteConfig {

    private Logger logger = LoggerFactory.getLogger(CamelRouteConfig.class);

    private List<String> messages = Collections.synchronizedList(new LinkedList<>());
    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @Bean
    public RouteBuilder route() {

        return new RouteBuilder() {
            public void configure() throws Exception {
//                String uri = format(
//                    "kafka:test?brokers=%s&maxPollRecords=%s&consumersCount=%s&seekTo=%s&groupId=%s",
//                                kafkaConfiguration.getBrokers(), kafkaConfiguration.getMaxPollRecords(),
//                                kafkaConfiguration.getConsumerCount(), kafkaConfiguration.getSeekTo(),
//                                kafkaConfiguration.getGroupId());

                String uri = format(
                    "kafka:test?brokers=%s&groupId=testing&autoOffsetReset=earliest&consumersCount=1",
                        kafkaConfiguration.getBrokers());
                logger.info("connecting to uri {}", uri);
                from(uri).process(new Processor() {
//                    .routeId("FromKafka")
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            String messageKey = "";
                            if (exchange.getIn() != null) {
                                Message message = exchange.getIn();
                                Integer partitionId = (Integer) message.getHeader(KafkaConstants.PARTITION);
                                String topicName = (String) message.getHeader(KafkaConstants.TOPIC);
                                if (message.getHeader(KafkaConstants.KEY) != null) {
                                    messageKey = (String) message.getHeader(KafkaConstants.KEY);
                                }
                                Object data = message.getBody();
                                logger.info("topicName={}, partitionId={}, messageKey={}, data='{}'",
                                    topicName, partitionId, messageKey, data);
                                messages.add(data.toString());
                            }
                        }
                    }
                ).to("log:input");
            }
        };
    }

    public List<String> messages() {
        return new ArrayList<>(messages);
    }
}
