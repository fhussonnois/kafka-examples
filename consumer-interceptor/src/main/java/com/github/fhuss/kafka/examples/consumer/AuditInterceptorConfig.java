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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class AuditInterceptorConfig extends AbstractConfig {

    public static final String AUDIT_APPLICATION_ID_CONFIG = "audit.application.id";
    public static final String AUDIT_APPLICATION_ID_DOC    = "The application id used to identify the producer";

    public static final String AUDIT_TOPIC_CONFIG  = "audit.topic";
    public static final String AUDIT_TOPIC_DOC = "The topic name";

    private static final ConfigDef CONFIG;
    public static final String AUDIT_PRODUCER_PREFIX = "audit.producer.";

    static {
        CONFIG = new ConfigDef()
                .define(AUDIT_APPLICATION_ID_CONFIG, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, AUDIT_APPLICATION_ID_DOC)
                .define(AUDIT_TOPIC_CONFIG, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, AUDIT_TOPIC_DOC);

    }

    /**
     * Creates a new {@link AuditInterceptorConfig} instance.
     *
     * @param originals the interceptor configuration.
     */
    AuditInterceptorConfig(final Map<String, ?> originals) {
        super(CONFIG, originals);
    }

    /**
     * Creates a new {@link AuditInterceptorConfig} instance.
     *
     * @param definition the {@link ConfigDef} instance.
     * @param originals  the interceptor configuration.
     */
    private AuditInterceptorConfig(final ConfigDef definition,
                                   final Map<String, ?> originals) {
        super(definition, originals);
    }

    public Map<String, Object> getOverrideProducerConfigs() {
        return originalsWithPrefix(AUDIT_PRODUCER_PREFIX);
    }


    public String getAuditTopic() {
        return this.getString(AUDIT_TOPIC_CONFIG);
    }

    public String getAuditApplicationId() {
        return this.getString(AUDIT_APPLICATION_ID_CONFIG);
    }
}
