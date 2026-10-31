package com.novindata.netflow.support;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import com.novindata.netflow.core.avro.NetFlowV2Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public final class AvroTestSerializer {

    private AvroTestSerializer() {
    }

    public static byte[] serialize(NetFlowV2Record record) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);

        new SpecificDatumWriter<>(NetFlowV2Record.class).write(record, encoder);
        encoder.flush();

        return output.toByteArray();
    }
}
