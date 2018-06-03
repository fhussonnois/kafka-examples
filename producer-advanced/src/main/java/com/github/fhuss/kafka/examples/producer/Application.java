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

import com.github.fhuss.kafka.examples.producer.services.FileProducerFailover;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Application {

    private static final Logger LOG = LogManager.getLogger(Application.class.getName());

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
    private static final String TOPIC = "test";

    private static CountDownLatch wait = new CountDownLatch(1);

    private static AtomicBoolean isClose = new AtomicBoolean(false);

    public static void main(String[] args) throws InterruptedException, IOException {

        // Add shutdown hook to respond to SIGTERM and gracefully stop the application.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Closing application gracefully (SIGTERM)");
                isClose.set(true);
                wait.await();
                System.out.println("Closed");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        KafkaProducer<String, String> producer = null;
        FileProducerFailover<String, String> fallback = null;
        try {
            // Create a new Producer with properties required to be idempotent.
            final Properties configs = newIdempotentProducerConfig("localhost:9092");
            producer = new KafkaProducer<>(configs);

            // Create a FileProducerFailover to log records on filesystem
            fallback = new FileProducerFailover<>(TEMP_DIR + "/kafka-records.json");

            ProducerService<String, String> service = new ProducerService<>(producer, fallback, false);
            LOG.info("Starting to produce records into topic {}", TOPIC);
            int count = 0;

            // Generate some data
            while (!isClose.get()) {
                try {
                    int i = ++count;
                    service.send("key-" + i, "value-" + i, TOPIC);
                    Thread.sleep(1000);
                } catch (ProducingException e) {
                    //e.printStackTrace();
                }
            }
            LOG.info("Stopped to produce records.");
        } finally {
            // Wait for pending records.
            producer.close(5, TimeUnit.SECONDS);
            fallback.close();
            wait.countDown();
        }

    }

    private static Properties newIdempotentProducerConfig(final String bootstrapServer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 16384 * 10);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return props;
    }
}
