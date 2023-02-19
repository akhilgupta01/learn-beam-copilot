package com.examples.beam.tx.steps;

import com.examples.beam.tx.TxJobOptions;
import com.examples.beam.tx.TxReportPipelineFactory;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TxReportStepDefs {
    private File inputFile;
    @Given("the following transactions are received")
    public void theFollowingTransactionsAreReceived(DataTable dataTable) throws IOException {
        List<String> dataRows = new ArrayList<>();
        dataTable.asMaps().forEach(row -> {
            StringBuilder sb = new StringBuilder();
            sb.append(row.get("txID")).append(",");
            sb.append(row.get("customerID")).append(",");
            sb.append(row.get("itemID")).append(",");
            sb.append(row.get("txTime")).append(",");
            sb.append(row.get("amount"));
            dataRows.add(sb.toString());
        });
        writeDataToInputFile(dataRows);
    }

    private void writeDataToInputFile(List<String> dataRows) throws IOException {
        try(FileWriter fileWriter = new FileWriter(createTempFile())) {
            for (String row : dataRows) {
                fileWriter.write(row);
                fileWriter.write(System.lineSeparator());
            }
        }
    }

    private File createTempFile() throws IOException {
        inputFile = File.createTempFile("tx" + System.currentTimeMillis(), ".csv");
        return inputFile;
    }

    @When("the customer transactions are aggregated")
    public void theCustomerTransactionsAreAggregated() {
        TxJobOptions options = PipelineOptionsFactory.as(TxJobOptions.class);
        options.setInputFile(inputFile.getAbsolutePath());
        options.setReportFile("target/report.csv");
        Pipeline pipeline = TxReportPipelineFactory.create(options);
        pipeline.run().waitUntilFinish();
    }

    @Then("the following records are generated")
    public void theFollowingRecordsAreGenerated(DataTable dataTable) {
        //do nothing
    
    }

    @Then("no records are generated")
    public void noRecordsAreGenerated() {
    }

    @And("the following transactions are captured in error")
    public void theFollowingTransactionsAreCapturedInError(DataTable dataTable) {
    }
}
