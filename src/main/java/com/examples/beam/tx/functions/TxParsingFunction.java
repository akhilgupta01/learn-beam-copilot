package com.examples.beam.tx.functions;

import com.examples.beam.tx.model.Transaction;
import org.apache.beam.sdk.transforms.DoFn;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TxParsingFunction extends DoFn<String, Transaction> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<Transaction> out) {
        try {
            String[] fields = line.split(",");
            Transaction tx = new Transaction(
                    fields[0],
                    fields[1],
                    fields[2],
                    LocalDateTime.parse(fields[3], DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    Double.parseDouble(fields[4]));
            out.output(tx);
        } catch (Exception e) {
            System.out.println("Error parsing line: " + line);
        }
    }
}
