package com.novindata.netflow.writer;

import com.novindata.netflow.core.validation.NetFlowValidationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

final class PartitionBatchProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionBatchProcessor.class);

    private final NetflowConverter converter;
    private final StarRocksClient starRocks;
    private final DlqPublisher dlqPublisher;

    PartitionBatchProcessor(
            NetflowConverter converter,
            StarRocksClient starRocks,
            DlqPublisher dlqPublisher) {
        this.converter = converter;
        this.starRocks = starRocks;
        this.dlqPublisher = dlqPublisher;
    }

    void process(List<ConsumerRecord<byte[], byte[]>> records) throws Exception {
        List<Row> validRows = new ArrayList<>();
        List<Future<RecordMetadata>> dlqWrites = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            try {
                validRows.add(new Row(record, converter.toCsv(record)));
            } catch (NetFlowValidationException validationFailure) {
                dlqWrites.add(dlqPublisher.publish(record,"VALIDATION_FAILED",validationFailure));
            } catch (Exception badRecord) {
                dlqWrites.add(dlqPublisher.publish(record, "INVALID_AVRO", badRecord));
            }
        }

        loadOrSplit(validRows, dlqWrites);
        awaitDlqWrites(dlqWrites);
    }

    private void loadOrSplit(
            List<Row> rows,
            List<Future<RecordMetadata>> dlqWrites) throws Exception {
        if (rows.isEmpty()) {
            return;
        }
        try {
            starRocks.load(rows.stream().map(Row::csv).toList(), converter.getCsvColumnsHeader(), label(rows));
        } catch (StarRocksClient.BadDataException badData) {
            if (rows.size() == 1) {
                Row rejected = rows.get(0);
                dlqWrites.add(dlqPublisher.publish(
                        rejected.source(), "STARROCKS_REJECTED", badData));
                return;
            }

            int middle = rows.size() / 2;
            List<Row> firstHalf = rows.subList(0, middle);
            List<Row> secondHalf = rows.subList(middle, rows.size());
            LOGGER.warn("StarRocks rejected batch; splitting: rows={}, firstHalf={}, secondHalf={}",
                    rows.size(), firstHalf.size(), secondHalf.size());
            loadOrSplit(firstHalf, dlqWrites);
            loadOrSplit(secondHalf, dlqWrites);
        }
    }

    private static void awaitDlqWrites(List<Future<RecordMetadata>> dlqWrites) throws Exception {
        for (Future<RecordMetadata> write : dlqWrites) {
            write.get();
        }
    }

    private static String label(List<Row> rows) {
        ConsumerRecord<byte[], byte[]> first = rows.get(0).source();
        ConsumerRecord<byte[], byte[]> last = rows.get(rows.size() - 1).source();
        return "netflow_p" + first.partition() + "_" + first.offset() + "_" + last.offset();
    }

    private record Row(ConsumerRecord<byte[], byte[]> source, String csv) {
    }
}
