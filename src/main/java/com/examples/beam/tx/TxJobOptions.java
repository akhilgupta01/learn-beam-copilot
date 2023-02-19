package com.examples.beam.tx;

import org.apache.beam.sdk.options.*;

public interface TxJobOptions extends PipelineOptions {
    @Description("Input transactions file")
    @Default.String("C:\\Users\\Akhil\\IdeaProjects\\learn-beam-copilot\\src\\main\\resources\\transactions.csv")
    @Validation.Required
    String getInputFile();
    void setInputFile(String value);

    @Description("Output Report")
    @Default.String("transaction_report.csv")
    String getReportFile();
    void setReportFile(String reportFile);
}
