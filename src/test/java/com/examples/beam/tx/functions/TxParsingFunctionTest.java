package com.examples.beam.tx.functions;

import com.examples.beam.tx.model.Transaction;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TxParsingFunctionTest {

    @Mock
    private DoFn.OutputReceiver<Transaction> outputReceiver;

    @Test
    public void shouldParseLine() {
        TxParsingFunction parsingFunction = new TxParsingFunction();
        String line = "tx1,cust1,item1,2020-01-01T00:00:00,100.0";
        parsingFunction.processElement(line, outputReceiver);
        ArgumentCaptor<Transaction> argumentCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(outputReceiver).output(argumentCaptor.capture());
        Transaction tx = argumentCaptor.getValue();
        assertEquals("tx1", tx.getTxId());
        assertEquals("cust1", tx.getCustomerId());
        assertEquals("item1", tx.getItemId());
        assertEquals(100.0, tx.getAmount(), 0.0);
    }
}
