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
package com.github.fhuss.kafka.examples.twitter;

import java.util.Properties;

public class TwitterClientConfig {

    public static String OAUTH_ACCESS_TOKEN_CONFIG      = "oauth.access.token";
    public static String OAUTH_ACCESS_SECRET_CONFIG     = "oauth.access.token.secret";
    public static String OAUTH_CONSUMER_KEY_CONFIG      = "oauth.consumer.key";
    public static String OAUTH_CONSUMER_SECRET_CONFIG   = "oauth.consumer.secret";
    public static String API_FILTER_TRACK               = "api.filter.track";

    private final Properties props;

    /**
     * Creates a new {@link TwitterClientConfig} instance.
     * @param props the config
     */
    public TwitterClientConfig(final Properties props) {
        this.props = props;
    }

    public String getOauthAccessTokenConfig() {
        return this.props.getProperty(OAUTH_ACCESS_TOKEN_CONFIG);
    }

    public String getOauthAccessSecretConfig() {
        return this.props.getProperty(OAUTH_ACCESS_SECRET_CONFIG);
    }

    public String getOauthConsumerKeyConfig() {
        return this.props.getProperty(OAUTH_CONSUMER_KEY_CONFIG);
    }

    public String getOauthConsumerSecretConfig() {
        return this.props.getProperty(OAUTH_CONSUMER_SECRET_CONFIG);
    }

    public String getApiFilterTrack() {
        return this.props.getProperty(API_FILTER_TRACK);
    }
}
