package org.arkcase.akrcasetimewindowing.configuration;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.arkcase.akrcasetimewindowing.service.TransactionTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.WindowedSerdes.timeWindowedSerdeFrom;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Mar, 2020
 */
@Configuration
public class KStreamsLdapsExample
{
    @Value(value = "${spring.kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;

    @Value(value = "${spring.kafka.streams.host}")
    private String host;

    @Value(value = "${spring.kafka.streams.state-dir}")
    private String stateDir;

    @Value(value = "${server.port}")
    private Integer serverPort;

    public static final Logger logger = LoggerFactory.getLogger(KStreamsLdapsExample.class);
    public static final String userAuthenticationStore = "user-authentication-store";

    private Topology buildTopology(final Map<String, String> serdeConfig)
    {
        final GenericAvroSerde valueSerde = new GenericAvroSerde();
        valueSerde.configure(serdeConfig, false);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> ldapAuthStream = builder.stream("ldap_auth_topic", Consumed.with(stringSerde, valueSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                .withTimestampExtractor(new TransactionTimestampExtractor()));

        KStream<String, GenericRecord> kerberosAuthStream = builder.stream("kerberos_auth_topic", Consumed.with(stringSerde, valueSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                .withTimestampExtractor(new TransactionTimestampExtractor()));

        KStream<String, GenericRecord> mergedAuthStream = ldapAuthStream.merge(kerberosAuthStream);


        mergedAuthStream
                .groupBy(( String key, GenericRecord value) -> value.get("tenantId").toString(), Grouped.with(Serdes.String(), valueSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)).grace(Duration.ofMinutes(5)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(userAuthenticationStore).withRetention(Duration.ofDays(40))
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde))
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .peek((key, value)
                -> logger.info("Key: {}, Number of records: {}", key.key(), value))
                .to("auth-result-topic", Produced.with(timeWindowedSerdeFrom(String.class), Serdes.Long()));

        Predicate<String, GenericRecord> isInternal =
        (key, record) -> record.get("tenantId").toString().equalsIgnoreCase("Armedia");

        Predicate<String, GenericRecord> isHDS =
                (key, record) -> !(record.get("tenantId").toString().equalsIgnoreCase("HDS"));

        int internal = 0;
        int hds = 1;
        KStream<String, GenericRecord>[] kstreamByOrganization = mergedAuthStream.branch(isInternal, isHDS);
        kstreamByOrganization[internal].to( "internal-authentications", Produced.with(stringSerde, valueSerde));
        kstreamByOrganization[hds].to("hds-authentications", Produced.with(stringSerde, valueSerde));

        return builder.build();
    }

    private Properties streamsConfig(final String bootstrapServers,
                                    final int applicationServerPort,
                                    final String stateDir,
                                    final String host)
    {
        final Properties streamsConfiguration = new Properties();
        // The name must be unique in the Kafka cluster against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "arkcase-auth-time-windowing-demo-store");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + applicationServerPort);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        return streamsConfiguration;
    }

    @Bean
    public KafkaStreams kafkaStreams()
    {
        KafkaStreams streams = new KafkaStreams(
                buildTopology(singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress)),
                streamsConfig(bootstrapAddress, serverPort, stateDir, host)
        );
        streams.start();
        return streams;
    }
}
