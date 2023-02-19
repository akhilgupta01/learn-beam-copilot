package com.examples.beam.tx.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class Transaction implements Serializable {
    private String txId;
    private String customerId;
    private String itemId;
    private LocalDateTime txTime;
    private double amount;
}
