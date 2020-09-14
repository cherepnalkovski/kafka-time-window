package org.arkcase.akrcasetimewindowing.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Apr, 2020
 */
public class TransactionTimestampExtractor implements TimestampExtractor
{
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime)
    {
        if (record.value() instanceof GenericRecord)
        {
            GenericRecord value = (GenericRecord) record.value();
            Schema.Field field = value.getSchema().getField("eventDate");
            if (field != null && field.schema().getType().equals(Schema.Type.STRING))
            {
                // Get the timestamp from the record value
                return Long.valueOf(value.get(field.pos()).toString());
            }
        }
        return record.timestamp();
    }
}
