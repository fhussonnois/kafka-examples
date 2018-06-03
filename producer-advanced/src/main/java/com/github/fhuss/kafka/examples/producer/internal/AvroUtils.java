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
package com.github.fhuss.kafka.examples.producer.internal;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

public class AvroUtils {

    private static final Logger LOG = LogManager.getLogger(AvroUtils.class.getName());

    private static final Charset CHARSET = Charset.forName("UTF-8");

    public static String convertGenericRecordToJson(final GenericRecord record) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
            datumWriter.write(record, jsonEncoder);
            jsonEncoder.flush();
            baos.flush();
            return new String(baos.toByteArray(), CHARSET);
        } catch (Exception e) {
            LOG.error("Can't encode generic record into json {}", e.getMessage());
        }

        // we failover to toString method that is supposed to provide a valid JSON
        // representation of the record.
        // However the record will not be valid to be read thought JSONDecoder class.
        return record.toString();
    }
}
