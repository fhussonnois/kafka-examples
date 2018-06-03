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

import com.github.fhuss.kafka.examples.producer.internal.Time;
import com.github.fhuss.kafka.examples.producer.internal.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class AuditProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static final String TRACKING_CORRELATION_ID         = "trackingCorrelationId";
    private static final String TRACKING_ON_SEND_EPOCH_TIME     = "trackingOnSendEpochTime";
    private static final String TRACKING_APPLICATION_ID         = "trackingApplicationId";

    // Record Metadata
    private static final String TRACKING_PARTITION              = "partition";
    private static final String TRACKING_OFFSET                 = "offset";
    private static final String TRACKING_TIMESTAMP              = "timestamp";
    private static final String TRACKING_TOPIC                  = "topic";
    private static final String TRACKING_SERIALIZED_KEY_SIZE_   = "serializedKeySize";
    private static final String TRACKING_SERIALIZED_VALUE_SIZE  = "serializedValueSize";

    private static final String JSON_OPEN_BRACKET = "{";
    private static final String JSON_CLOSE_BRACKET = "}";

    private String originalsClientId;

    private AuditInterceptorConfig configs;

    private Producer<String, String> producer;

    /**
     * {@inheritDoc}
     */
    @Override
    public ProducerRecord onSend(ProducerRecord<K, V> record) {
        record.headers()
                .add(TRACKING_CORRELATION_ID, CorrelationIdGenerator.getId())
                .add(TRACKING_APPLICATION_ID, configs.getAuditApplicationId().getBytes(CHARSET))
                .add(TRACKING_ON_SEND_EPOCH_TIME, Utils.longToBytes(Time.SYSTEM.milliseconds()));
        return record;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            final String value = getJsonTrackingMessage(metadata);
            producer.send(new ProducerRecord<>(configs.getAuditTopic(), value));
        }
    }

    private String getJsonTrackingMessage(RecordMetadata metadata) {
        return JSON_OPEN_BRACKET +
                "\"" + "timestamp" + "\":\"" + Time.SYSTEM.milliseconds() + "\"" +
                "\",client\":" +
                JSON_OPEN_BRACKET +
                "\"" + "clientId" + "\":\"" + originalsClientId + "\"" +
                ",\"" + "applicationId" + "\":\"" + configs.getAuditApplicationId() + "\"" +
                ",\"" + "type" + "\":\"producer\"" +
                JSON_CLOSE_BRACKET +
                ",\"record\":" +
                JSON_OPEN_BRACKET +
                "\"" + TRACKING_PARTITION + "\":\"" + metadata.partition() + "\"" +
                ",\"" + TRACKING_TOPIC + "\":\"" + metadata.topic() + "\"" +
                ",\"" + TRACKING_SERIALIZED_KEY_SIZE_ + "\":\"" + metadata.serializedKeySize() + "\"" +
                ",\"" + TRACKING_SERIALIZED_VALUE_SIZE + "\":\"" + metadata.serializedValueSize() + "\"" +
                ",\"" + TRACKING_OFFSET + "\":\"" + (metadata.hasOffset() ? metadata.offset() : -1) + "\"" +
                ",\"" + TRACKING_TIMESTAMP + "\":\"" + (metadata.hasTimestamp() ? metadata.timestamp() : -1) + "\"" +
                JSON_CLOSE_BRACKET +
                JSON_CLOSE_BRACKET;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {

        final Map<String, Object> copyConfigs = new HashMap<>(configs);
        // Drop interceptor classes configuration to not introduce loop.
        copyConfigs.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);

        this.originalsClientId = (String) configs.get(ProducerConfig.CLIENT_ID_CONFIG);

        String interceptorClientId = (originalsClientId == null) ?
                "interceptor-producer-" + ClientIdGenerator.nextClientId() :
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

    private static class CorrelationIdGenerator {

        private static final Charset CHARSET = Charset.forName("UTF-8");

       static byte[] getId() {
           return UUID.randomUUID().toString().getBytes(CHARSET);
       }
    }
}
