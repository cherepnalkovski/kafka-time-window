package org.arkcase.akrcasetimewindowing.configuration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@arkcase.com> on Dec, 2019
 */
@Configuration
public class KafkaTopicConfiguration
{
    @Value(value = "${spring.kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.topic.ldap-auth}")
    private String ldap_auth_topic;

    @Value(value = "${spring.kafka.topic.kerberos-auth}")
    private String kerberos_auth_topic;

    @Bean
    public KafkaAdmin kafkaAdmin()
    {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic ldapTopic() {
        return new NewTopic(ldap_auth_topic, 1, (short) 1);
    }

    @Bean
    public NewTopic kerberosTopic() {
        return new NewTopic(kerberos_auth_topic, 1, (short) 1);
    }
}
