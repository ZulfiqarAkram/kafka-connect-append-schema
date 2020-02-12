package com.github.yousufdev.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class AppendSchemaTest {

    private AppendSchema<SinkRecord> xform = new AppendSchema.Value<>();

    @AfterEach
    public void tearDown() { xform.close(); }


    @Test
    public void schemaless() {
        String serializedSchema = "{'schema':{'type':'struct','fields':[{'type':'int32','optional':false,'field':'id'},{'type':'string','optional':false,'field':'name'}]}}";

        final Map<String, Object> value = new HashMap<>();
        value.put("id", 1);
        value.put("name", "john");

        xform.configure(Collections.singletonMap("schema", serializedSchema));

        final SinkRecord record = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.valueSchema().field("id").schema());
        assertEquals(1, ((Struct) transformedRecord.value()).getInt32("id").intValue());

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("john", ((Struct) transformedRecord.value()).getString("name") );

    }

}