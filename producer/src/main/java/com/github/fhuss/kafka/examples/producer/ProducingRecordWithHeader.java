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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Example : Add header to producer records.
 */
public class ProducingRecordWithHeader {

    private static final String CORRELATION_ID_HEADER = "correlationId";

    public static void main(String[] args) {

        Producer<String, String> producer = new KafkaProducer<>(newProducerConfig());

        // Create a new record
        final ProducerRecord<String, String> record = new ProducerRecord<>("topic-example", "key", "value");

        // Add a simple headers
        record.headers().add(CORRELATION_ID_HEADER, CorrelationIdGenerator.getId());

        // Send record
        producer.send(record, new PrintProducerCallback());
    }

    private static Properties newProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return props;
    }

    static class CorrelationIdGenerator {

        private static final Charset CHARSET = Charset.forName("UTF-8");

        static byte[] getId() {
            return UUID.randomUUID().toString().getBytes(CHARSET);
        }
    }
}
