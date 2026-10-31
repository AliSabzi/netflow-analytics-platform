package com.novindata.netflow.support;

import com.novindata.netflow.core.avro.NetFlowV2Record;

public final class NetFlowTestData {

    private NetFlowTestData() {
    }

    public static NetFlowV2Record validRecord() {
        NetFlowV2Record record = new NetFlowV2Record();
        record.setIPV4SRCADDR("10.0.0.1");
        record.setIPV4DSTADDR("10.0.0.2");
        return record;
    }
}
