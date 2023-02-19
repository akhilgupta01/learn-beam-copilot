package com.examples.beam.tx;

import com.examples.beam.tx.functions.TxAggregatingFunction;
import com.examples.beam.tx.functions.TxParsingFunction;
import com.examples.beam.tx.model.Transaction;
import com.examples.beam.tx.model.TxReport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class TxReportPipelineFactory {
    public static Pipeline create(TxJobOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read Transactions", TextIO.read().from(options.getInputFile()))
                .apply("Parse Transactions", ParDo.of(new TxParsingFunction()))
                .apply("Map To Account", MapElements.via(new SimpleFunction<Transaction, KV<String, Transaction>>() {
                    @Override
                    public KV<String, Transaction> apply(Transaction tx) {
                        return KV.of(tx.getCustomerId(), tx);
                    }
                }))
                .apply("Group By CustomerId", GroupByKey.create())
                .apply("Aggregate By Customer", ParDo.of(new TxAggregatingFunction()))
                .apply("Map To CSV", MapElements.via(new SimpleFunction<TxReport, String>() {
                    @Override
                    public String apply(TxReport report) {
                        return report.getCustomerId() + "," + report.getTotalTx() + "," + report.getTotalAmount();
                    }
                }))
                .apply("Write Report", TextIO.write().to(options.getReportFile()).withoutSharding());
        return pipeline;
    }
}
