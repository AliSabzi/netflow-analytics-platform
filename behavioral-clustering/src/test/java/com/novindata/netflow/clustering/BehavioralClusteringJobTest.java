package com.novindata.netflow.clustering;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BehavioralClusteringJobTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("BehavioralClusteringJobTest")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void buildsOneFeatureRowPerSourceIp() {
        Dataset<Row> features = new BehavioralClusteringJob(spark).buildFeatures(createNetflowData());

        assertEquals(8, features.count());
        assertEquals(9, features.columns().length);
    }

    private static Dataset<Row> createNetflowData() {
        StructType schema = new StructType()
                .add("IPV4_SRC_ADDR", DataTypes.StringType)
                .add("IPV4_DST_ADDR", DataTypes.StringType)
                .add("L4_DST_PORT", DataTypes.IntegerType)
                .add("IN_BYTES", DataTypes.LongType)
                .add("OUT_BYTES", DataTypes.LongType)
                .add("IN_PKTS", DataTypes.LongType)
                .add("OUT_PKTS", DataTypes.LongType)
                .add("RETRANSMITTED_IN_PKTS", DataTypes.LongType)
                .add("RETRANSMITTED_OUT_PKTS", DataTypes.LongType)
                .add("FLOW_DURATION_MILLISECONDS", DataTypes.LongType);

        List<Row> rows = new ArrayList<>();
        for (int ip = 1; ip <= 8; ip++) {
            boolean busy = ip > 4;
            int flowCount = busy ? 8 + ip : 2 + ip;
            for (int flow = 0; flow < flowCount; flow++) {
                long factor = busy ? 100L : 1L;
                rows.add(RowFactory.create(
                        "10.0.0." + ip,
                        "192.168." + ip + "." + flow,
                        busy ? 1000 + flow : 80,
                        factor * (100 + flow),
                        factor * (200 + flow),
                        10L + flow,
                        20L + flow,
                        busy ? 2L : 0L,
                        busy ? 1L : 0L,
                        factor * (50 + flow)
                ));
            }
        }
        return spark.createDataFrame(rows, schema);
    }
}
