package com.novindata.netflow;

import com.novindata.netflow.service.LateDataJob;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public final class LateDataApplication {

    private LateDataApplication() {
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        SpringApplication application = new SpringApplication(Application.class);
        application.setAdditionalProfiles("late-job");
        try (ConfigurableApplicationContext context = application.run(args)) {
            context.getBean(LateDataJob.class).run();
        }
    }
}
