package org.arkcase.akrcasetimewindowing.service;

import org.apache.avro.generic.GenericRecord;
import org.arkcase.akrcasetimewindowing.configuration.KafkaProducerConfiguration;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Date;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Feb, 2020
 */
@Service
public class ProducerService
{
    private final KafkaProducerConfiguration kafkaProducerConfiguration;

    public ProducerService(KafkaProducerConfiguration kafkaProducerConfiguration)
    {
        this.kafkaProducerConfiguration = kafkaProducerConfiguration;
    }

    public void sendMessage(GenericRecord event, Long timestamp, String topic)
    {
        ListenableFuture<SendResult<String, GenericRecord>> future =
                kafkaProducerConfiguration.kafkaTemplate().send(topic, null, timestamp, event.get("id").toString(), event);

        future.addCallback(new ListenableFutureCallback<SendResult<String, GenericRecord>>() {

            @Override
            public void onSuccess(SendResult<String, GenericRecord> result) {
                System.out.println("Sent message=[" + event.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "] " + " Datetime: " + new Date(timestamp).toString());
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + event.toString() + "] due to : " + ex.getMessage());
            }
        });
    }
}
