package com.novindata.netflow.clustering;

import org.apache.spark.ml.linalg.Vectors;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterDescriptorTest {

    @Test
    void describesTheThreeStrongestDeviations() {
        ClusterDescriptor.Description description = ClusterDescriptor.describe(
                Vectors.dense(0.1, -2.4, 0.2, 1.8, 0.3, 0.4, 1.2, 0.0)
        );

        assertEquals("total bytes", description.firstFeature());
        assertEquals(-2.4, description.firstDeviation());
        assertEquals("unique destinations", description.secondFeature());
        assertEquals("retransmission ratio", description.thirdFeature());
        assertTrue(description.label().contains("low total bytes (-2.40 SD)"));
    }
}
