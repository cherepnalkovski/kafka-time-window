package org.arkcase.akrcasetimewindowing.configuration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueJoiner;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Apr, 2020
 */
public class GenericRedordsJoiner implements ValueJoiner<GenericRecord, GenericRecord, GenericRecord>
{
    private static final Schema resultSchema = new Schema.Parser().parse("some schema definition");

    @Override
    public GenericRecord apply(GenericRecord left, GenericRecord right) {
        GenericRecord projectionRecord = new GenericData.Record(resultSchema);
        projectionRecord.put("id", left.get("id"));
        projectionRecord.put("userId", left.get("userId"));
        projectionRecord.put("name", right.get("name"));
        projectionRecord.put("lastName", right.get("lastName"));
        return projectionRecord;
    }

}
