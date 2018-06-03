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
package com.github.fhuss.kafka.examples.producer.services;

import com.github.fhuss.kafka.examples.producer.internal.Times;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.fhuss.kafka.examples.producer.internal.AvroUtils.convertGenericRecordToJson;

public class FileProducerFailover<K, V> implements ProducerFailover<K, V>, Closeable {

    private static final Logger LOG = LogManager.getLogger(FileProducerFailover.class.getName());

    private FallbackWriter writer;

    /**
     * Creates a new {@link FileProducerFailover} instance.
     *
     * @param path  the path of the file used to write records.
     */
    public FileProducerFailover(final String path) throws IOException {
        this(Paths.get(path));
    }

    /**
     * Creates a new {@link FileProducerFailover} instance.
     *
     * @param path  the path of the file used to write records.
     */
    private FileProducerFailover(final Path path) throws IOException {
        Objects.requireNonNull(path, "Path cannot be null");
        Files.createDirectories(path.getParent());
        final String parent = path.getParent().toAbsolutePath().toString();
        this.writer = new FallbackWriter(parent, path.getFileName().toString());
        this.writer.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void failover(ProducerRecord<K, V> record) {
        String toWrite = null;
        try {
            V value = record.value();
            if (value instanceof GenericRecord) {
                toWrite = convertGenericRecordToJson((GenericRecord) value);
            } else {
                toWrite = value.toString();
            }
            this.writer.write(toWrite);
        } catch (IOException e) {
            LOG.error("Can't write record to local disk {} : {} ", e.getMessage(), toWrite);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    private static class FallbackWriter extends Thread implements Closeable {

        private static final Logger LOG = LogManager.getLogger(FallbackWriter.class.getName());

        private static final Charset CHARSET = Charset.forName("UTF-8");

        private static final int CLEANER_INTERVAL_MS = 5;

        private static DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.of("UTC"));

        private final CountDownLatch shutdownLatch = new CountDownLatch(1);

        private volatile long lastAppend = Times.now();

        private BufferedWriter writer;

        private final Object mut = new Object();

        private final String baseDirectory;

        private final String baseFilename;

        private Path writerPath;

        FallbackWriter(final String baseDirectory, final String baseFilename) {
            Objects.requireNonNull(baseDirectory, "baseDirectory cannot be null");
            Objects.requireNonNull(baseDirectory, "baseFilename cannot be null");
            this.baseDirectory = baseDirectory;
            this.baseFilename = baseFilename;
            this.setName("file-cleaner-thread");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            LOG.info("Starting failover-file cleaner thread");
            while (!isClosed()) {
                try {
                    maybeRollFile();
                    boolean shuttingDown = shutdownLatch.await(CLEANER_INTERVAL_MS, TimeUnit.MILLISECONDS);
                    if (shuttingDown) {
                        return;
                    }
                } catch (InterruptedException e) {
                    LOG.error("Unexpected InterruptedException, ignoring: ", e);
                } catch (Exception e) {
                    LOG.warn("Unexpected error occurred while rolling failover file : {}", writerPath.toString());
                }
            }
            LOG.info("Fallback-file cleaner thread stopped");
        }

        private boolean isClosed() {
            return shutdownLatch.getCount() == 0;
        }

        void write(final String record) throws IOException {
            if (isClosed()) throw new IllegalStateException("Cannot write failover file after close");
            long now = Times.now();
            synchronized (mut) {
                maybeRollFile();
                if (writer == null) {
                    this.writerPath = createAndGetPath();
                    this.writer = Files.newBufferedWriter(this.writerPath, CHARSET, StandardOpenOption.APPEND);
                } else {
                    writer.newLine();
                }
                writer.write(record);
                writer.flush();
                lastAppend = now;
            }
        }

        private void maybeRollFile() throws IOException {
            long now = Times.now();
            if (now - lastAppend > 60 * 1000 && writer != null) {
                LOG.info("Rolling failover file : {}", "");
                synchronized (mut) {
                    writer.close();
                    writer = null;
                }
            }
        }

        private Path createAndGetPath() throws IOException {
            Instant now = Instant.now();
            String date = FORMATTER.format(now);
            Path path = null;
            int index = 0;
            while (path == null) {
                String filename = String.format("%s-%05d-%s", date, index, this.baseFilename);
                Path fallbackPath = Paths.get(this.baseDirectory, filename);
                if (!Files.exists(fallbackPath)) {
                    LOG.info("Creating new failover file : {}", fallbackPath);
                    Files.createFile(fallbackPath);
                    path = fallbackPath;
                } else {
                    index++;
                }
            }
            return path;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            LOG.info("Closing FileProducerFailover service");
            shutdownLatch.countDown();
            if (writer != null) {
                synchronized (mut) {
                    writer.close();
                }
            }
            LOG.info("FileProducerFailover service close");
        }
    }
}
