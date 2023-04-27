package com.big.data.engineering3.pipelines.domain;

import com.big.data.engineering3.pipelines.PubSubToGcsOptions;
import com.big.data.engineering3.pipelines.VleWriteCsvPerWindow;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "pipeline.vle", name = "enabled", havingValue = "true")
public class VlePipeline implements CommandLineRunner {

    @Override
    public void run(String... args) throws Exception {
        run();
    }

    public void run() {

        log.info("Starting VlePipeline");
        String[] argsTo = new String[]{"--project=gifted-loader-384715",
                "--region=australia-southeast1",
                "--inputTopic=projects/gifted-loader-384715/topics/vle-topic",
                "--output=gs://zone_landing_ebd_nus_2023/des_raw_csv/7_vle",
                "--gcpTempLocation=gs://dataflow_files_ebd2023/studentInfoTemp",
                "--runner=DataflowRunner",
                "--windowSize=1",
                "--serviceAccount=poc-608@gifted-loader-384715.iam.gserviceaccount.com"
        };

        // The maximum number of shards when writing output.
        int numShards = 1;
        PubSubToGcsOptions options =
                PipelineOptionsFactory.fromArgs(argsTo).withValidation().as(PubSubToGcsOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                // 2) Group the messages into fixed-sized minute intervals.
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
                // 3) Write one file to GCS for every window of messages.
                .apply("Write Files to GCS", new VleWriteCsvPerWindow(options.getOutput(), numShards));

        // Execute the pipeline and wait until it finishes running.
//        pipeline.run().waitUntilFinish();
        pipeline.run();
        log.info("Done VlePipeline");
    }
}
