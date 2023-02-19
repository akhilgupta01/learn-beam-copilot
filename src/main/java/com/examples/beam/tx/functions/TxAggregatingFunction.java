package com.examples.beam.tx.functions;

import com.examples.beam.tx.model.Transaction;
import com.examples.beam.tx.model.TxReport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class TxAggregatingFunction extends DoFn<KV<String, Iterable<Transaction>>, TxReport> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<Transaction>> customerTransactions, OutputReceiver<TxReport> out) {
        TxReport report = new TxReport();
        report.setCustomerId(customerTransactions.getKey());
        double totalAmount = 0;
        int totalTx = 0;
        for (Transaction tx : customerTransactions.getValue()) {
            totalAmount += tx.getAmount();
            totalTx++;
        }
        report.setTotalAmount(totalAmount);
        report.setTotalTx(totalTx);
        out.output(report);
    }
}
