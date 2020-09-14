package org.arkcase.akrcasetimewindowing.configuration;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.arkcase.akrcasetimewindowing.service.TransactionTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Mar, 2020
 */
//@Configuration
public class KTableConfiguration
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

    public static final String oneDayStore = "one-minute-new-windowed-store-1";
    public static final String sevenDaysStore = "seven-minutes-new-windowed-store-1";
    public static final String thirtyDaysStore = "thirty-minutes-new-windowed-store-1";
    private static final String ldapUsers = "ldap_users_topic";

    private static final Schema resultSchema = new Schema.Parser().parse("some schema definition");
    static Topology buildTopology(final Map<String, String> serdeConfig)
    {
        final GenericAvroSerde valueSerde = new GenericAvroSerde();
        valueSerde.configure(serdeConfig, false);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> ldapAuth = builder.stream("windowing-by-day-01", Consumed.with(stringSerde, valueSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                .withTimestampExtractor(new TransactionTimestampExtractor()));

        KTable<String, GenericRecord> userTable = builder.table("user-topic", Consumed.with(stringSerde, valueSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                .withTimestampExtractor(new TransactionTimestampExtractor()));

        // Only if there is ldap user with user id in user table, record will be produced.
        KStream<String, GenericRecord> result = ldapAuth.join(userTable, new GenericRedordsJoiner());

        result
        .to(ldapUsers, Produced.with(stringSerde, valueSerde));

        return builder.build();
    }

    static Properties streamsConfig(final String bootstrapServers,
                                    final int applicationServerPort,
                                    final String stateDir,
                                    final String host)
    {
        final Properties streamsConfiguration = new Properties();
        // The name must be unique in the Kafka cluster against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowing-by-days-01-new-topic-example");
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
