package com.novindata.netflow.core;

import org.junit.jupiter.api.Test;
import com.novindata.netflow.core.avro.NetFlowV2OutputRecord;
import com.novindata.netflow.core.avro.NetFlowV2Record;
import com.novindata.netflow.core.validation.NetFlowValidationException;
import com.novindata.netflow.core.validation.NetFlowValidator;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetFlowContractTest {
    @Test
    void inputRecordResolvesToValidOutputRecord() throws Exception {
        NetFlowV2OutputRecord record = new NetFlowV2OutputRecord();
        record.setIPV4SRCADDR("10.0.0.1");
        record.setIPV4DSTADDR("10.0.0.2");


        assertNull(record.getPROCESSINGTIMESTAMP());
        assertTrue(NetFlowValidator.validate(record));
    }

    @Test
    void validatorAcceptsValidInputRecord() {
        NetFlowV2Record record = new NetFlowV2Record();
        record.setIPV4SRCADDR("10.0.0.1");
        record.setIPV4DSTADDR("10.0.0.2");

        assertTrue(NetFlowValidator.validate(record));
    }

    @Test
    void validatorRejectsInvalidOutoutSourceAddress() {
        NetFlowV2OutputRecord record = new NetFlowV2OutputRecord();
        record.setIPV4SRCADDR("999.0.0.1");
        record.setIPV4DSTADDR("10.0.0.2");

        NetFlowValidationException error = assertThrows(
                NetFlowValidationException.class,
                () -> NetFlowValidator.validate(record));

        assertTrue(error.getMessage().startsWith("IPV4_SRC_ADDR is not a valid IPv4 address"));
    }

    @Test
    void validatorRejectsInvalidInputSourceAddress() {
        NetFlowV2Record record = new NetFlowV2Record();
        record.setIPV4SRCADDR("999.0.0.1");
        record.setIPV4DSTADDR("10.0.0.2");

        NetFlowValidationException error = assertThrows(
                NetFlowValidationException.class,
                () -> NetFlowValidator.validate(record));

        assertTrue(error.getMessage().startsWith("IPV4_SRC_ADDR is not a valid IPv4 address"));
    }
}
