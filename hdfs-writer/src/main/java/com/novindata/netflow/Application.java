package com.novindata.netflow;

import com.novindata.netflow.monitoring.PipelineMetrics;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class Application {

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        int metricsPort = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", "9400"));
        PipelineMetrics.start(metricsPort);

        Runtime.getRuntime().addShutdownHook(new Thread(PipelineMetrics::stop, "metrics-shutdown"));

        try {
            SpringApplication.run(Application.class, args);
            PipelineMetrics.markReady();
        } catch (RuntimeException e) {
            PipelineMetrics.stop();
            throw e;
        }
    }
}
