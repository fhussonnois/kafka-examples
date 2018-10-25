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

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TwitterClient implements Closeable {


    private static final int DEFAULT_CAPACITY = 1000;

    private final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<>(DEFAULT_CAPACITY);

    private final TwitterStream twitterStream;

    private AtomicBoolean isStarted = new AtomicBoolean(false);

    private final TwitterClientConfig config;

    /**
     * Creates a new {@link TwitterClient} instance.
     * @param config
     */
    TwitterClient(final TwitterClientConfig config) {
        this.config = config;
        Configuration build = getConfiguration(config);
        this.twitterStream = new TwitterStreamFactory(build).getInstance();
        twitterStream.addListener(new DefaultStatusListener());
    }

    private Configuration getConfiguration(final TwitterClientConfig config) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(config.getOauthConsumerKeyConfig())
                .setOAuthConsumerSecret(config.getOauthConsumerSecretConfig())
                .setOAuthAccessToken(config.getOauthAccessTokenConfig())
                .setOAuthAccessTokenSecret(config.getOauthAccessSecretConfig());
        return cb.build();
    }

    /**
     * Creates a new {@link TwitterClient} instance.
     * @param props
     */
    public TwitterClient(final Properties props) {
        this(new TwitterClientConfig(props));
    }


    public Collection<Status> next() {

        if (!isStarted.get()) {
            FilterQuery query = new FilterQuery().track(config.getApiFilterTrack());
            twitterStream.filter(query);
            this.isStarted.set(true);
        }


        if (!queue.isEmpty()) {
            Collection<Status> status = new ArrayList<>(queue.size());
            queue.drainTo(status);

            return status;
        }

        return Collections.emptyList();
    }

    @Override
    public void close() {
        this.twitterStream.shutdown();
    }

    public class DefaultStatusListener implements StatusListener {

        @Override
        public void onStatus(Status status) {
            TwitterClient.this.queue.offer(status);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {

        }

        @Override
        public void onStallWarning(StallWarning warning) {

        }

        @Override
        public void onException(Exception ex) {

        }
    }
}
