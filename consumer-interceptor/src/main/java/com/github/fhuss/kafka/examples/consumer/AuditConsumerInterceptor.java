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

import com.github.fhuss.kafka.examples.consumer.internal.Time;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AuditConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    // Record Metadata
    private static final String TRACKING_PARTITION              = "partition";
    private static final String TRACKING_OFFSET                 = "offset";
    private static final String TRACKING_TIMESTAMP              = "timestamp";
    private static final String TRACKING_TOPIC                  = "topic";

    private static final String JSON_OPEN_BRACKET = "{";
    private static final String JSON_CLOSE_BRACKET = "}";

    private String originalsClientId;

    private AuditInterceptorConfig configs;

    private Producer<String, String> producer;

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(r -> {
            final String value = getJsonTrackingMessage(r);
            producer.send(new ProducerRecord<>(configs.getAuditTopic(), value));
        });
        return null;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    private String getJsonTrackingMessage(ConsumerRecord<K, V> record) {
        return JSON_OPEN_BRACKET +
                "\"" + "timestamp" + "\":\"" + Time.SYSTEM.milliseconds() + "\"" +
                "\",client\":" +
                JSON_OPEN_BRACKET +
                "\"" + "clientId" + "\":\"" + originalsClientId + "\"" +
                ",\"" + "applicationId" + "\":\"" + configs.getAuditApplicationId() + "\"" +
                ",\"" + "type" + "\":\"consumer\"" +
                JSON_CLOSE_BRACKET +
                ",\"record\":" +
                JSON_OPEN_BRACKET +
                "\"" + TRACKING_PARTITION + "\":\"" + record.partition() + "\"" +
                ",\"" + TRACKING_TOPIC + "\":\"" + record.topic() + "\"" +
                ",\"" + TRACKING_OFFSET + "\":\"" + record.offset() + "\"" +
                ",\"" + TRACKING_TIMESTAMP + "\":\"" + record.timestamp() + "\"" +
                JSON_CLOSE_BRACKET +
                JSON_CLOSE_BRACKET;
    }

    @Override
    public void close() {
        if (this.producer != null) {
            try {
                this.producer.close();
            } catch (InterruptException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final Map<String, Object> copyConfigs = new HashMap<>(configs);
        // Drop interceptor classes configuration to not introduce loop.
        copyConfigs.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);

        this.originalsClientId = (String) configs.get(ProducerConfig.CLIENT_ID_CONFIG);

        String interceptorClientId = (originalsClientId == null) ?
                "interceptor-consumer-" + ClientIdGenerator.nextClientId() :
                "interceptor-" + originalsClientId;

        copyConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, interceptorClientId);

        this.configs = new AuditInterceptorConfig(copyConfigs);

        copyConfigs.putAll(this.configs.getOverrideProducerConfigs());

        // Enforce some properties to get a non-blocking producer;
        copyConfigs.put(ProducerConfig.RETRIES_CONFIG, "0");
        copyConfigs.put(ProducerConfig.ACKS_CONFIG, "1");
        copyConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "0");
        copyConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        copyConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(copyConfigs);
    }

    private static class ClientIdGenerator {

        private static final AtomicInteger IDS = new AtomicInteger(0);

        static int nextClientId() {
            return IDS.getAndIncrement();
        }
    }
}
