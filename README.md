# Kafka Examples

This repository contains small examples demonstrating Producer/Consumer API and Kafka Streams features.

## Producer API

### Producer

* `ProducingSimpleRecord` : Demonstrates how to use the Java Producer API to send record into Kafka.
* `ProducingRecordWithHeader` : Demonstrates how to add header to a record before sending it into Kafka. 

### Producer-Advanced

Complete example demonstrating how to implement a failover mechanism using the Callback interface in order to don't loss any record even in case of cluster failure.

### Producer-Interceptor

* `ProducerWithInterceptor` : Demonstrates how to implement a custom producer interceptor in order to track all records being sent.

### Producer Transactional

* `ProducerWithInterceptor` : Demonstrates how to implement a transactional producer.

## Consumer API

### Consumer-Interceptor

* `ConsumerWithInterceptor` : Demonstrates how to implement a custom consumer interceptor in order to track all records being fetched.

## Kafka Stream 

Work in progress!

## Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License