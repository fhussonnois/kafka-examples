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
package com.github.fhuss.kafka.examples.producer.services;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Default interface used to write {@link ProducerRecord} to a failover storage
 * in case of a producer not retriable exception.
 *
 * @param <K>   the record-key type.
 * @param <V>   the record-value type.
 */
public interface ProducerFailover<K, V> {

    /**
     * This method should invoked from a producer {@link org.apache.kafka.clients.producer.Callback}
     * when an exception happen.
     *
     * @param record  the record that cannot be sent.
     */
    void failover(final ProducerRecord<K, V> record);

    /**
     * Default {@link ProducerFailover} implementation that print records on System.out.
     *
     * @param <K>   the record-key type.
     * @param <V>   the record-value type.
     */
    class LogProducerFailover<K, V> implements ProducerFailover<K, V> {
        @Override
        public void failover(final ProducerRecord<K, V> record) {
            System.out.println(record);
        }
    }

}
