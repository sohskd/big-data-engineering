package com.big.data.engineering3.pipelines;

import org.apache.beam.sdk.options.*;

/*
 * Define your own configuration options. Add your own arguments to be processed
 * by the command-line parser, and specify default values for them.
 */
public interface PubSubToGcsOptions extends PipelineOptions, StreamingOptions {

    @Description("The Cloud Pub/Sub topic to read from.")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Output file's window size in number of minutes.")
    @Default.Integer(1)
    Integer getWindowSize();

    void setWindowSize(Integer value);

    @Description("Path of the output file including its filename prefix.")
    @Validation.Required
    String getOutput();

    void setOutput(String output);
}
