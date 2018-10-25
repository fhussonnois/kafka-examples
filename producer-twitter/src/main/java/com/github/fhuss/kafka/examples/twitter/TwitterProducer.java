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

import com.github.fhuss.kafka.examples.twitter.domain.Tweet;
import com.github.fhuss.kafka.examples.twitter.domain.TweetBuilder;
import com.jsoniter.output.JsonStream;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.Status;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TwitterProducer {

    private static final Logger LOG = LogManager.getLogger(TwitterProducer.class);


    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    private final TwitterClient twitterClient;

    private final String topic;

    private final Producer<String, String> producer;

    /**
     * Creates a new {@link TwitterProducer} instance.
     */
    TwitterProducer(final String topic,
                    final TwitterClient client,
                    final Producer<String, String> producer) {
        this.topic = topic;
        this.twitterClient = client;
        this.producer = producer;
    }

    void sendStatus(final Status status) {
        final Tweet tweet = TweetBuilder.from(status);
        String tweetAsJson = JsonStream.serialize(tweet);
        LOG.info("{}", tweetAsJson);
        this.producer.send(new ProducerRecord<>(this.topic, status.getUser().getName(), tweetAsJson), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOG.error("Unexpected error occurred while sending record {}", tweetAsJson);
                }
            }
        });
    }

    public void start() {
        try {
            while (!isShutdown.get()) {
                Collection<Status> status = twitterClient.next();
                status.forEach(this::sendStatus);
                if (status.isEmpty()) {
                    Thread.sleep(500);
                }
            }

        } catch (Exception e) {
            LOG.error(e);
        } finally {
            this.producer.close();
            this.twitterClient.close();
            latch.countDown();
        }
    }

    public void stop(){
        LOG.info("Stopping TwitterProducer");
        this.isShutdown.set(true);
        try {
            this.latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
        }
        LOG.info("TwitterProducer closed");
    }
}
