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

import com.github.fhuss.kafka.examples.producer.services.ProducerFailover;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default class to encapsulate {@link org.apache.kafka.clients.producer.KafkaProducer} instance.
 *
 * @param <K>   the record-key type.
 * @param <V>   the record-value type.
 */
public class ProducerService<K, V> {

    private static final Logger LOG = LogManager.getLogger(ProducerService.class.getName());

    private Producer<K, V> producer;

    private boolean closeOnError;

    private final ProducerFailover<K, V> fallback;

    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new {@link ProducerService} instance.
     *
     * @param producer      the Kafka client producer to use
     * @param fallback      the instance of {@link ProducerFailover} to use in case of not retriable exception.
     * @param closeOnError  close the producer on the first exception
     */
    ProducerService(final Producer<K, V> producer,
                    final ProducerFailover<K, V> fallback,
                    final boolean closeOnError) {
        this.producer = producer;
        this.closeOnError = closeOnError;
        this.fallback = fallback;
    }

    /**
     * Send a new key-value pair to the specified topic.
     *
     * @param key       the record-key
     * @param value     the record-value
     * @param topic     the target topic name
     *
     * @throws ProducingException if the record cannot be add to the producer client buffer and so cannot be send to Kafka.
     */
    public void send(final K key, final V value, final String topic) throws ProducingException {

        try {
            ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
            this.producer.send(record, new FallbackCallback(record));

        // Callback is not invoked on those exceptions
        } catch (KafkaException e) {
            LOG.warn("Unexpected error occurred while sending record with key={}, value={} to topic={}", key, value, topic);
            throw new ProducingException(e);
        } catch (Exception e) {
            LOG.warn("Unexpected error occurred while sending record with key={}, value={} to topic={}", key, value, topic);
            throw new ProducingException(e);
        }
    }

    private class FallbackCallback implements Callback {

        private ProducerRecord<K, V> record;

        /**
         * Creates a new {@link FallbackCallback} instance.
         *
         * @param record  a {@link ProducerRecord} instance.
         */
        FallbackCallback(final ProducerRecord<K, V> record) {
            this.record = record;
        }

        @Override
        public void onCompletion(final RecordMetadata metadata, final Exception exception) {

            if (exception != null) {
                // If leaders are unreachable record batches will be expired after request.timeout.ms for
                // all partitions for which there is no in flight request.
                // This may lead to records re-ordering if the application can continue to send new records after expiring buffered batches.
                // For this reason the producer client is closed on the first callback.
                if (closeOnError && !closed.get()) {
                    ProducerService.this.producer.close(0, TimeUnit.SECONDS);
                    closed.set(true);
                }
                fallback.failover(record);
            }
        }
    }
}
