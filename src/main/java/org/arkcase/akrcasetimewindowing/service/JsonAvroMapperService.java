package org.arkcase.akrcasetimewindowing.service;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Feb, 2020
 */
@Service
public class JsonAvroMapperService
{
    private final JsonAvroConverter converter;
    private final SchemaService schemaService;

    public JsonAvroMapperService(SchemaService schemaService)
    {
        this.schemaService = schemaService;
        this.converter = new JsonAvroConverter();
    }

    /** GenericRecord to json converter
     *
     * @param record - Avro serialized GenericRecord
     * @return - JSON String
     */
    public String convertToJson(GenericRecord record)
    {
        if(record != null)
        {
            byte[] binaryJson = converter.convertToJson(record);
            return new String(binaryJson);
        }
        return "Unable to convert empty record.";
    }

    /** Json to GenericRecord converter
     *
     * @param jsonMessage - json to be converted
     * @param schemaName - schema name for the conversion
     * @return Avro serialized GenericRecord.
     * @throws IOException
     */
    public GenericData.Record convertToAvro(String jsonMessage, String schemaName) throws IOException, RestClientException
    {
        Schema userSchema = getSchemaFromSchemaServer(schemaName);

        // conversion to GenericData.Record
        return converter.convertToGenericDataRecord(jsonMessage.getBytes(), userSchema);
    }

    /** Get Schema from resources
     *
     * @param schemaPath - location of the schema
     * @return - Avro schema object
     * @throws IOException
     */
    private Schema getSchema(String schemaPath) throws IOException
    {
        return new Schema.Parser().parse(new ClassPathResource(schemaPath).getInputStream());
    }

    /**
     *
     * @param schemaName
     * @return - Avro schema object
     * @throws IOException
     */
    private Schema getSchemaFromSchemaServer(String schemaName) throws IOException, RestClientException
    {
        String schemaByNameAndVersion = schemaService.getLatestSchemaByName(schemaName);

        Schema schema = new Schema.Parser().parse(schemaByNameAndVersion);
        return schema;
    }

    /** Example: how to manually create GenericRecord with composition from Avro schema.
     *
     * @return - GenericRecord
     * @throws IOException
     */
    private GenericRecord createGenericObject() throws IOException
    {
        Schema userSchema = getSchema("avro/User.avsc");

        //Create User record
        GenericRecord user = new GenericData.Record(userSchema);

        user.put("id", UUID.randomUUID().toString());
        user.put("username", "ann-acm@armedia.com");

        //Create Audit Record
        GenericRecord userAudit = new GenericData.Record(userSchema.getField("audit").schema());
        userAudit.put("userId", UUID.randomUUID().toString());
        userAudit.put("ipAddress", "ann-acm@armedia.com");
        userAudit.put("requestId", UUID.randomUUID().toString());

        // Add Audit to User record
        user.put("audit", userAudit);

        return user;
    }
}
