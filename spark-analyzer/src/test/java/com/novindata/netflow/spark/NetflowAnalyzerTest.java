package com.novindata.netflow.spark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.time.LocalDateTime;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class NetflowAnalyzerTest {
    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .appName("NetflowAnalyzerTest")
                .master("local[1]")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown(){
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void testProtocolUsage()
    {
        StructType schema = new StructType()
                .add("PROTOCOL", DataTypes.IntegerType)
                .add("IN_BYTES", DataTypes.LongType)
                .add("OUT_BYTES", DataTypes.LongType);

        Dataset<Row> input = spark.createDataFrame(
                List.of(
                        RowFactory.create(6, 100L, 50L),
                        RowFactory.create(6, 200L, 100L),
                        RowFactory.create(17, 50L, 25L)
                ), schema
        );

        LocalDateTime dateTime = LocalDateTime.of(2026, 1, 1, 0, 0);
        Dataset<Row> result = NetflowAnalyzer.getProtocolsUsage(input, dateTime);

        Row protocol6 = result.filter("protocol = 6").first();
        assertEquals(450L,protocol6.getLong(protocol6.fieldIndex("total_bytes")));
        assertEquals(2L,protocol6.getLong(protocol6.fieldIndex("flow_count")));

        Row protocol17 = result.filter("protocol = 17").first();
        assertEquals(75L,protocol17.getLong(protocol17.fieldIndex("total_bytes")));
        assertEquals(1L,protocol17.getLong(protocol17.fieldIndex("flow_count")));
    }

    @Test
    void testIpConnections()
    {
        StructType schema = new StructType()
                .add("IPV4_SRC_ADDR", DataTypes.StringType)
                .add("IPV4_DST_ADDR", DataTypes.StringType)
                .add("IN_BYTES", DataTypes.LongType)
                .add("OUT_BYTES", DataTypes.LongType);

        Dataset<Row> input = spark.createDataFrame(
                List.of(
                        RowFactory.create("10.0.0.1", "10.0.0.2", 100L, 50L),
                        RowFactory.create("10.0.0.1", "10.0.0.2", 200L, 100L),
                        RowFactory.create("10.0.0.2", "10.0.0.1", 300L, 150L),
                        RowFactory.create("10.0.0.1", "10.0.0.3", 50L, 25L)
                ), schema
        );

        LocalDateTime dateTime = LocalDateTime.of(2026, 1, 1, 0, 0);
        Dataset<Row> result = NetflowAnalyzer.getIpConnections(input,dateTime);

        assertEquals(3,result.count());

        Row connection1 = result.filter("src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'").first();
        assertEquals(300L,connection1.getLong(connection1.fieldIndex("in_bytes")));
        assertEquals(150L,connection1.getLong(connection1.fieldIndex("out_bytes")));
        assertEquals(2L,connection1.getLong(connection1.fieldIndex("flow_count")));

        Row connection2 = result.filter("src_ip = '10.0.0.1' AND dst_ip = '10.0.0.3'").first();
        assertEquals(50L,connection2.getLong(connection2.fieldIndex("in_bytes")));
        assertEquals(25L,connection2.getLong(connection2.fieldIndex("out_bytes")));
        assertEquals(1L,connection2.getLong(connection2.fieldIndex("flow_count")));

        Row connection3 = result.filter("src_ip = '10.0.0.2' AND dst_ip = '10.0.0.1'").first();
        assertEquals(300L,connection3.getLong(connection3.fieldIndex("in_bytes")));
        assertEquals(150L,connection3.getLong(connection3.fieldIndex("out_bytes")));
        assertEquals(1L,connection3.getLong(connection3.fieldIndex("flow_count")));
    }

    @Test
    void testIpBandwidths()
    {
        StructType schema = new StructType()
                .add("IPV4_SRC_ADDR", DataTypes.StringType)
                .add("IPV4_DST_ADDR", DataTypes.StringType)
                .add("IN_BYTES", DataTypes.LongType)
                .add("OUT_BYTES", DataTypes.LongType);

        Dataset<Row> input = spark.createDataFrame(
                List.of(
                        RowFactory.create("10.0.0.1", "10.0.0.2", 100L, 50L),
                        RowFactory.create("10.0.0.1", "10.0.0.2", 200L, 100L),
                        RowFactory.create("10.0.0.2", "10.0.0.1", 300L, 150L),
                        RowFactory.create("10.0.0.3", "10.0.0.1", 50L, 25L)
                ), schema
        );

        LocalDateTime dateTime = LocalDateTime.of(2026, 1, 1, 0, 0);
        Dataset<Row> result = NetflowAnalyzer.getIpBandwidths(input,dateTime);

        assertEquals(3,result.count());

        Row ip1 = result.filter("ip = '10.0.0.1'").first();
        assertEquals(500L,ip1.getLong(ip1.fieldIndex("in_bytes")));
        assertEquals(475L,ip1.getLong(ip1.fieldIndex("out_bytes")));

        Row ip2 = result.filter("ip = '10.0.0.2'").first();
        assertEquals(450L,ip2.getLong(ip2.fieldIndex("in_bytes")));
        assertEquals(450L,ip2.getLong(ip2.fieldIndex("out_bytes")));

        Row ip3 = result.filter("ip = '10.0.0.3'").first();
        assertEquals(25L,ip3.getLong(ip3.fieldIndex("in_bytes")));
        assertEquals(50L,ip3.getLong(ip3.fieldIndex("out_bytes")));
    }
}