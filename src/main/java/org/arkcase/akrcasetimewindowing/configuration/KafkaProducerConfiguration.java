package org.arkcase.akrcasetimewindowing.configuration;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@arkcase.com> on Dec, 2019
 */
@Configuration
public class KafkaProducerConfiguration
{
    @Value(value = "${spring.kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;

    @Bean
    public ProducerFactory<String, GenericRecord> producerFactory()
    {
        return new DefaultKafkaProducerFactory<>(getProperties());
    }

    @Bean
    public KafkaTemplate<String, GenericRecord> kafkaTemplate()
    {
        return new KafkaTemplate<>(producerFactory());
    }

    private Map<String, Object> getProperties()
    {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(5));
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress);

        return configProps;
    }
}
