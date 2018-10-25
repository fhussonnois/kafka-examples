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
package com.github.fhuss.kafka.examples.twitter.domain;

import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;

public class TweetBuilder {

    /**
     * Creates a new {@link Tweet} instance from the specified {@link Status}.
     * @param status    the status used to construct the tweet.
     * @return          a new {@link Tweet} instance.
     */
    public static Tweet from(final Status status) {
        GeoLocation location = status.getGeoLocation();
        User user = status.getUser();
        return new Tweet(status.getText(),
                status.getCreatedAt().getTime(),
                status.getId(),
                (location != null) ? location.getLatitude() : null,
                (location != null) ? location.getLongitude() : null,
                status.getLang(),
                user.getName(),
                user.getLocation(),
                user.getURL(),
                user.getProfileImageURL());
    }
}
