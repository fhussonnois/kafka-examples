/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.examples.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Simple producer using an interceptor to track records sent.
 */
public class ProducerWithInterceptor {

    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws InterruptedException {

        Producer<String, String> producer = new KafkaProducer<>(newProducerConfig());

        // Add shutdown hook to respond to SIGTERM and gracefully stop the application.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing application gracefully (SIGTERM)");
            latch.countDown();
            producer.close();
            System.out.println("Closed");
        }));

        while (latch.getCount() > 0) {
            producer.send(new ProducerRecord<>("my-topic", "my-key", "my-value"), (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {

                    System.out.printf("Successfully send record to topic=%s, partition=%s with offset=%d\n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            if (latch.await(1, TimeUnit.SECONDS)) {
                break;
            }
        }
        System.out.println("Stop to produce records");
    }

    private static Properties newProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Configure interceptor and attached configuration.
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, AuditProducerInterceptor.class.getName());
        props.put(AuditInterceptorConfig.AUDIT_TOPIC_CONFIG, "tracking-clients");
        props.put(AuditInterceptorConfig.AUDIT_APPLICATION_ID_CONFIG, "kafka-clients-examples");
        return props;
    }
}
