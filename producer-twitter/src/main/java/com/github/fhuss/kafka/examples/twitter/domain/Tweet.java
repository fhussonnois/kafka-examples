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

import java.util.Objects;

public class Tweet {

    private String text;
    private long createdAt;
    private long id;
    private Double geoLatitude;
    private Double geoLongitude;
    private String lang;
    private String userName;
    private String userLocation;
    private String userUrl;
    private String userProfileImageURL;

    /**
     * Dummy constructor for deserialization.
     */
    public Tweet() {

    }

    /**
     * Creates a new {@link Tweet} instance.
     */
    public Tweet(final String text,
                 final long createdAt,
                 final long id,
                 final Double geoLatitude,
                 final Double geoLongitude,
                 final String lang,
                 final String userName,
                 final String userLocation,
                 final String userUrl,
                 final String userProfileImageURL) {
        this.text = text;
        this.createdAt = createdAt;
        this.id = id;
        this.geoLatitude = geoLatitude;
        this.geoLongitude = geoLongitude;
        this.lang = lang;
        this.userName = userName;
        this.userLocation = userLocation;
        this.userUrl = userUrl;
        this.userProfileImageURL = userProfileImageURL;
    }


    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Double getGeoLatitude() {
        return geoLatitude;
    }

    public void setGeoLatitude(Double geoLatitude) {
        this.geoLatitude = geoLatitude;
    }

    public Double getGeoLongitude() {
        return geoLongitude;
    }

    public void setGeoLongitude(Double geoLongitude) {
        this.geoLongitude = geoLongitude;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserLocation() {
        return userLocation;
    }

    public void setUserLocation(String userLocation) {
        this.userLocation = userLocation;
    }

    public String getUserUrl() {
        return userUrl;
    }

    public void setUserUrl(String userUrl) {
        this.userUrl = userUrl;
    }

    public String getUserProfileImageURL() {
        return userProfileImageURL;
    }

    public void setUserProfileImageURL(String userProfileImageURL) {
        this.userProfileImageURL = userProfileImageURL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tweet tweet = (Tweet) o;
        return createdAt == tweet.createdAt &&
                id == tweet.id &&
                Double.compare(tweet.geoLatitude, geoLatitude) == 0 &&
                Double.compare(tweet.geoLongitude, geoLongitude) == 0 &&
                Objects.equals(text, tweet.text) &&
                Objects.equals(lang, tweet.lang) &&
                Objects.equals(userName, tweet.userName) &&
                Objects.equals(userLocation, tweet.userLocation) &&
                Objects.equals(userUrl, tweet.userUrl) &&
                Objects.equals(userProfileImageURL, tweet.userProfileImageURL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {

        return Objects.hash(text, createdAt, id, geoLatitude, geoLongitude, lang, userName, userLocation, userUrl, userProfileImageURL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Tweet{");
        sb.append("text='").append(text).append('\'');
        sb.append(", createdAt=").append(createdAt);
        sb.append(", id=").append(id);
        sb.append(", geoLatitude=").append(geoLatitude);
        sb.append(", geoLongitude=").append(geoLongitude);
        sb.append(", lang='").append(lang).append('\'');
        sb.append(", userName='").append(userName).append('\'');
        sb.append(", userLocation='").append(userLocation).append('\'');
        sb.append(", userUrl='").append(userUrl).append('\'');
        sb.append(", userProfileImageURL='").append(userProfileImageURL).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
