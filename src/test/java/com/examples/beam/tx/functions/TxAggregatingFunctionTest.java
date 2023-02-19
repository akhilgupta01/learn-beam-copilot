package com.examples.beam.tx.functions;

import com.examples.beam.tx.model.Transaction;
import com.examples.beam.tx.model.TxReport;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class TxAggregatingFunctionTest {
    @Mock
    private DoFn.OutputReceiver<TxReport> outputReceiver;

    @Test
    public void aggregatesTransactions() {
        TxAggregatingFunction txAggregatingFunction = new TxAggregatingFunction();
        Iterable<Transaction> txs = Create.of(
                new Transaction("tx1", "cust1", "item1", LocalDateTime.now(), 100.0),
                new Transaction("tx2", "cust1", "item2", LocalDateTime.now(),200.0),
                new Transaction("tx3", "cust1", "item3", LocalDateTime.now(),300.0)
        ).getElements();
        KV<String, Iterable<Transaction>> customerTransactions = KV.of("cust1", txs);
        txAggregatingFunction.processElement(customerTransactions, outputReceiver);
        ArgumentCaptor<TxReport> argumentCaptor = ArgumentCaptor.forClass(TxReport.class);
        verify(outputReceiver).output(argumentCaptor.capture());
        TxReport report = argumentCaptor.getValue();
        assertEquals("cust1", report.getCustomerId());
        assertEquals(600.0, report.getTotalAmount(), 0.0);
        assertEquals(3, report.getTotalTx());
    }
}
