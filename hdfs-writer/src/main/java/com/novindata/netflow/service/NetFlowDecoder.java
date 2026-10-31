package com.novindata.netflow.service;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class NetFlowDecoder {

    private final KafkaAvroDeserializer deserializer;
    private Schema currentSchema;

    public NetFlowDecoder(
            @Value("${app.schema-registry.url}") String schemaRegistryUrl) {

        deserializer = new KafkaAvroDeserializer();
        deserializer.configure(Map.of
                ("schema.registry.url", schemaRegistryUrl,"specific.avro.reader", false),false);
    }

    public synchronized GenericRecord decodeAndValidate(String topic,byte[] value) {

        GenericRecord input = (GenericRecord) deserializer.deserialize(topic, value);
        currentSchema = merge(currentSchema, input.getSchema());
        GenericRecord output = new GenericData.Record(currentSchema);
        for (Schema.Field field : currentSchema.getFields()) {
            Schema.Field inputField = input.getSchema().getField(field.name());

            output.put( field.name(),inputField == null ? null : input.get(field.name()));
        }
        return output;
    }

    private Schema merge(Schema current, Schema incoming) {

        if (current == null) {
            return makeNullable(incoming);
        }

        boolean hasNewField = incoming.getFields().stream().anyMatch(field ->current.getField(field.name()) == null);
        if (!hasNewField) {
            return current;
        }

        List<Schema.Field> fields = new ArrayList<>();

        for (Schema.Field field : current.getFields()) {
            fields.add(new Schema.Field(field.name(),field.schema(),field.doc(),null));
        }

        for (Schema.Field field : incoming.getFields()) {
            if (current.getField(field.name()) == null) {
                fields.add(new Schema.Field(field.name(),nullable(field.schema()),field.doc(),null));
            }
        }
        Schema merged = Schema.createRecord(current.getName(), current.getDoc(),current.getNamespace(),false);
        merged.setFields(fields);
        return merged;
    }

    private Schema makeNullable(Schema schema) {
        Schema result = Schema.createRecord(
                schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                false
        );

        List<Schema.Field> fields = new ArrayList<>();

        for (Schema.Field field : schema.getFields()) {
            fields.add(new Schema.Field(
                    field.name(),
                    nullable(field.schema()),
                    field.doc(),
                    null
            ));
        }

        Schema timestampSchema = LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG));

        fields.add(new Schema.Field("KAFKA_RECORD_TIMESTAMP",nullable(timestampSchema),
                null,null));

        fields.add(new Schema.Field("KAFKA_TOPIC",nullable(Schema.create(Schema.Type.STRING)),
                null,null));

        fields.add(new Schema.Field("KAFKA_PARTITION",nullable(Schema.create(Schema.Type.INT)),
                null,null));

        fields.add(new Schema.Field("KAFKA_OFFSET",nullable(Schema.create(Schema.Type.LONG)),
                null,null));

        fields.add(new Schema.Field("KAFKA_TIMESTAMP_TYPE",nullable(Schema.create(Schema.Type.STRING)),
                null,null));

        result.setFields(fields);
        return result;
    }

    private Schema nullable(Schema schema) {
        if (schema.isNullable()) {
            return schema;
        }
        return Schema.createUnion(Schema.create(Schema.Type.NULL),schema);
    }
}