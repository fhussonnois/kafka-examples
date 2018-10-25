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

import com.github.fhuss.kafka.examples.twitter.internal.CLIUtils;
import com.github.fhuss.kafka.examples.twitter.internal.Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TweetProducerApplication {

    private static final String TWITTER_CONFIG_KEY = "twitter";
    private static final String PRODUCER_CONFIG_KEY = "producer";

    public static void main(String[] args) {

        TwitterProducerOptions options = new TwitterProducerOptions(args);
        if(args.length == 0) {
            CLIUtils.printUsageAndDie(options.parser, "Simple Application to track and publish tweets into Kafka");
        }

        options.checkArgs();


        int exitCode = 0;
        try {
            Config config = ConfigFactory.parseFile(options.configPropsFileOpt());
            TwitterClient twitterClient = newTwitterClient(config);

            Producer<String, String> producer = newProducer(config, options.bootstrapServerOpt());
            TwitterProducer application = new TwitterProducer(options.topic(), twitterClient, producer);
            Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
            application.start();

        } catch (Exception e) {
            e.printStackTrace();
            exitCode = 1;
        } finally {
            System.exit(exitCode);
        }
    }

    private static Producer<String, String> newProducer(final Config config, final String bootstrapServers) {
        Config producerConfig = config.getConfig(PRODUCER_CONFIG_KEY);

        Properties props = Utils.toProperties(producerConfig);
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static TwitterClient newTwitterClient(final Config config) {
        TwitterClientConfig clientConfig = new TwitterClientConfig(Utils.toProperties(config.getConfig(TWITTER_CONFIG_KEY)));
        return new TwitterClient(clientConfig);
    }
}
