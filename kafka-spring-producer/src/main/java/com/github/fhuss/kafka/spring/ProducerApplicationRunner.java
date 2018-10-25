package com.github.fhuss.kafka.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class ProducerApplicationRunner implements ApplicationRunner {

    @Autowired
    private KafkaTemplate<String, String> template;

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplicationRunner.class, args).close();
    }

    @Override
    public void run(ApplicationArguments args) {
        sendRecordToTopic("testing", "I");
        sendRecordToTopic("testing", "Heart");
        sendRecordToTopic("testing", "Logs");
        sendRecordToTopic("testing", "Event Data");
        sendRecordToTopic("testing", "Stream Processing");
        sendRecordToTopic("testing", "and Data Integration");
        this.template.flush();
    }

    private void sendRecordToTopic(String topic, String value) {
        ListenableFuture<SendResult<String, String>> future = this.template.send(topic, value);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println(result);
            }

            @Override
            public void onFailure(Throwable ex) {
                System.err.println(ex.getMessage());
            }

        });
    }
}
