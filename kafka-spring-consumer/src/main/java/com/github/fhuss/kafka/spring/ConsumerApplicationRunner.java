package com.github.fhuss.kafka.spring;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ConsumerApplicationRunner implements ApplicationRunner {

    private final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(4);

    @Autowired
    private ConsumerFactory<String, String> cf;

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplicationRunner.class, args).close();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        ContainerProperties containerProperties = new ContainerProperties("testing");
        containerProperties.setConsumerRebalanceListener(consumerRebalanceListener());
        containerProperties.setAckMode(AbstractMessageListenerContainer.AckMode.RECORD);
        containerProperties.setPollTimeout(Integer.MAX_VALUE);

        containerProperties.setMessageListener((MessageListener<String, String>) record -> {
            System.out.printf("[%s] Consumed new message : key=%s, value=%s, partition=%s, offset=%s\n", Thread.currentThread().getName(),
                    record.key(),
                    record.value(),
                    record.partition(),
                    record.offset());
            COUNT_DOWN_LATCH.countDown();
        });

        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProperties);

        container.setClientIdSuffix("spring-consumer-container");
        container.start();

        if (COUNT_DOWN_LATCH.await(1, TimeUnit.MINUTES)) {
            System.out.println("Messages received");
        } else {
            System.out.println("Timeout before receiving all messages");
        }
        container.stop(new Runnable() {
            @Override
            public void run() {

            }
        });
    }

    private ConsumerRebalanceListener consumerRebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Revoked : " + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("Assigned : " + partitions);
            }
        };
    }
}
