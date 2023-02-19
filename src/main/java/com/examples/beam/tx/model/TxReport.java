package com.examples.beam.tx.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class TxReport implements Serializable {
    private String customerId;
    private double totalAmount;
    private int totalTx;
}
