package com.examples.beam.tx;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class TxReportJobRunner {
    public static void main(String[] args) {
        TxJobOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TxJobOptions.class);
        Pipeline pipeline = TxReportPipelineFactory.create(options);
        pipeline.run();
    }

}
