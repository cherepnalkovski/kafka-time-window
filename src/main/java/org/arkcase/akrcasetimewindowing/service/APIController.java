package org.arkcase.akrcasetimewindowing.service;

import com.google.gson.JsonObject;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.arkcase.akrcasetimewindowing.configuration.KStreamsLdapsExample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import tech.allegro.schema.json2avro.converter.AvroConversionException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Mar, 2020
 */
@RestController
public class APIController
{
    private final String ldapAuthTopic;
    private final String kerberosAuthTopic;
    private final ProducerService producerService;
    private final JsonAvroMapperService jsonAvroMapperService;
    private final KafkaStreams kafkaStreams;

    private  final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public APIController(@Value("${spring.kafka.topic.ldap-auth}") String ldapAuthTopic,@Value("${spring.kafka.topic.kerberos-auth}") String kerberosAuthTopic, ProducerService producerService, JsonAvroMapperService jsonAvroMapperService, KafkaStreams kafkaStreams)
    {
        this.ldapAuthTopic = ldapAuthTopic;
        this.kerberosAuthTopic = kerberosAuthTopic;
        this.producerService = producerService;
        this.jsonAvroMapperService = jsonAvroMapperService;
        this.kafkaStreams = kafkaStreams;
    }

    @PostMapping("/insertMessage/{tenant}/{auth}")
    private void insertMessage(@PathVariable("tenant") String tenant, @PathVariable("auth") String auth)
    {
        if(auth.equals("ldap"))
        {
            sendMessage(tenant, ldapAuthTopic, "ldap_auth_topic-value");
        }
        else
        {
            sendMessage(tenant, kerberosAuthTopic, "kerberos_auth_topic-value");
        }
    }

    @GetMapping("/get/day/{tenant}/{days}")
    public ResponseEntity<?> getByDay(@PathVariable("tenant") String tenant, @PathVariable("days") Long days)
    {
        final ReadOnlyWindowStore<String, Long> dayStore = kafkaStreams.store(KStreamsLdapsExample.userAuthenticationStore, QueryableStoreTypes.windowStore());

        Instant timeFrom = (Instant.now().minus(Duration.ofDays(days)));

        LocalDate currentDate = LocalDate.now();
        LocalDateTime currentDayTime = currentDate.atTime(23, 59, 59);
        Instant timeTo = Instant.ofEpochSecond(currentDayTime.toEpochSecond(ZoneOffset.UTC));

        try(WindowStoreIterator<Long> iterator = dayStore.fetch(tenant, timeFrom, timeTo))
        {
            Long count = 0L;
            JsonObject jsonObject = new JsonObject();
            while (iterator.hasNext())
            {
                final KeyValue<Long, Long> next = iterator.next();
                Date resultDate = new Date(next.key);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                jsonObject.addProperty(format.format(resultDate), next.value);
                count += next.value;
                logger.info("Count of 'Auth Events' per day @ time " + currentDate + " is " + next.value);
            }

            jsonObject.addProperty("tenant", tenant);
            jsonObject.addProperty("Total number of events", count);

            return ResponseEntity.ok(jsonObject.toString());
        }
    }

    public void sendMessage(String tenant, String topic, String schemaName)
    {
        JsonObject jsonObject = MessageGeneratorNext.create(tenant);

        GenericRecord event = null;

        try
        {
            event = jsonAvroMapperService.convertToAvro(jsonObject.toString(), schemaName);
        }
        catch (IOException | RestClientException e)
        {
            logger.error("Unable to create AVRO generic object.");
            throw  new AvroConversionException(e.getMessage());
        }

        producerService.sendMessage(event, Long.valueOf(event.get("eventDate").toString()), topic);
    }
}
