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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TxReportStepDefs {
    private String inputFilePath;
    private String reportFilePath;

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
        try (FileWriter fileWriter = new FileWriter(createTempFile())) {
            for (String row : dataRows) {
                fileWriter.write(row);
                fileWriter.write(System.lineSeparator());
            }
        }
    }

    private File createTempFile() throws IOException {
        long runKey = System.currentTimeMillis();
        File inputFile = File.createTempFile("tx" + runKey, ".csv");
        inputFilePath = inputFile.getAbsolutePath();
        reportFilePath = Paths.get(inputFile.getParentFile().getPath(), "report" + runKey + ".csv").toString();
        return inputFile;
    }

    @When("the customer transactions are aggregated")
    public void theCustomerTransactionsAreAggregated() {
        TxJobOptions jobOptions = PipelineOptionsFactory.as(TxJobOptions.class);
        jobOptions.setInputFile(inputFilePath);
        jobOptions.setReportFile(reportFilePath);
        Pipeline pipeline = TxReportPipelineFactory.create(jobOptions);
        pipeline.run().waitUntilFinish();
    }

    @Then("the following records are generated")
    public void theFollowingRecordsAreGenerated(DataTable dataTable) throws IOException {
        CSVFormat csvFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setSkipHeaderRecord(false)
                .build();
        CSVParser csvParser = new CSVParser(new FileReader(reportFilePath), csvFormat);
        List<CSVRecord> actualRecords = csvParser.getRecords();
        dataTable.asMaps().forEach(row -> {
            actualRecords.stream().filter(record -> record.get(0).equals(row.get("customerID"))).findFirst().ifPresent(record -> {
                assertEquals(Double.parseDouble(row.get("totalTransactions")), Double.parseDouble(record.get(1)), 0.0);
                assertEquals(Double.parseDouble(row.get("totalAmount")), Double.parseDouble(record.get(2)), 0.0);
            });
        });

    }

    @Then("no records are generated")
    public void noRecordsAreGenerated() throws IOException {
        CSVFormat csvFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setSkipHeaderRecord(false)
                .build();
        CSVParser csvParser = new CSVParser(new FileReader(reportFilePath), csvFormat);
        List<CSVRecord> actualRecords = csvParser.getRecords();
        assertEquals(0, actualRecords.size());
    }

    @And("the following transactions are captured in error")
    public void theFollowingTransactionsAreCapturedInError(DataTable dataTable) {
    }
}
