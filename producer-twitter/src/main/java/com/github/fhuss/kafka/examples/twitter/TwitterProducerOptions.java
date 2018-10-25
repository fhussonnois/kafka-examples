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
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.File;

/**
 *
 */
public class TwitterProducerOptions {

    private static final String BootstrapServerArg = "bootstrap-server";
    private static final String BootstrapServerDoc = "REQUIRED: The server to connect to.";

    private static final String ConfigArg = "config";
    private static final String ConfigDoc = "REQUIRED: The config file containing  twitter and kafka producer props.";

    private static final String TopicArg = "file";
    private static final String TopicDoc = "REQUIRED:  The cluster specification to used for the command.";

    private final OptionSet options;

    private final ArgumentAcceptingOptionSpec<File> configOpt;

    private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;

    private final ArgumentAcceptingOptionSpec<String> topicOpt;

    public final OptionParser parser;

    /**
     * Creates a new {@link TwitterProducerOptions} instance.
     *
     * @param args the arguments
     */
    TwitterProducerOptions(String[] args) {
        this.parser = new OptionParser(false);

        topicOpt = parser.accepts(TopicArg, TopicDoc)
                .withRequiredArg()
                .describedAs("the target topic")
                .ofType(String.class);

        configOpt = parser.accepts(ConfigArg, ConfigDoc)
                .withRequiredArg()
                .describedAs("config property file")
                .ofType(File.class);

        bootstrapServerOpt = parser.accepts(BootstrapServerArg, BootstrapServerDoc)
                .withRequiredArg()
                .describedAs("server(s) to use for bootstrapping")
                .ofType(String.class);
        options = parser.parse(args);
    }

    public void checkArgs() {
        CLIUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
        CLIUtils.checkRequiredArgs(parser, options, topicOpt);
        CLIUtils.checkRequiredArgs(parser, options, configOpt);
    }

    public String topic() {
        return topicOpt.value(options);
    }

    public String bootstrapServerOpt() {
        return this.bootstrapServerOpt.value(options);
    }

    public File configPropsFileOpt() {
        return this.configOpt.value(options);
    }

}