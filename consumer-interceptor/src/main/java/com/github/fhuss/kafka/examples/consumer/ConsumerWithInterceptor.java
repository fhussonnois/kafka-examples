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
package com.github.fhuss.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerWithInterceptor {

    private static final AtomicBoolean closed = new AtomicBoolean(false);

    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {

        final Properties configs = newConsumerConfigs("localhost:9092", "group-1");
        final Consumer<String, String> consumer = new KafkaConsumer<>(configs);

        // Add shutdown hook to respond to SIGTERM and gracefully stop the application.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing application gracefully (SIGTERM)");
            closed.set(true);
            consumer.wakeup();
            try {
                latch.await();
            } catch (InterruptedException ignore) {
            }
            System.out.println("Closed");
        }));

        try {
            consumer.subscribe(Collections.singleton("my-topic"));
            consumer.poll(0); // trigger partition assignments.

            // Starting consumption
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Integer.MAX_VALUE);
                // Handle new records
                if (records != null) {
                    records.forEach(r -> {
                        System.out.printf("Consumed record : key=%s, value=%s", r.key(), r.value());
                    });
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            System.out.println("Closing consumer");
            consumer.close();
            latch.countDown();
        }
    }

    static Properties newConsumerConfigs(final String bootstrapServer,
                                         final String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Configure interceptor and attached configuration.
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, AuditConsumerInterceptor.class.getName());
        props.put(AuditInterceptorConfig.AUDIT_TOPIC_CONFIG, "tracking-clients");
        props.put(AuditInterceptorConfig.AUDIT_APPLICATION_ID_CONFIG, "kafka-clients-examples");
        return props;

    }
}
