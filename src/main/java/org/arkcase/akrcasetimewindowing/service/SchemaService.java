package org.arkcase.akrcasetimewindowing.service;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Mar, 2020
 */
@Service
public class SchemaService
{
    @Value(value = "${spring.kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;

    private final RestTemplate restTemplate;
    private SchemaRegistryClient schemaRegistryClient;

    public SchemaService(RestTemplate restTemplate)
    {
        this.restTemplate = restTemplate;
    }

    public String getLatestSchemaByName(String schemaName) throws IOException, RestClientException
    {
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryAddress, 1000);
        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(schemaName);
        return schemaMetadata.getSchema();
    }
}
