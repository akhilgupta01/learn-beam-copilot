Feature: Aggregates customer transactions and reports total sale amount and number of transactions for each customer.

  Scenario: Multiple transactions for a customer
    Given the following transactions are received
      | customerID | transactionID | amount | txTime              | itemID   |
      | cust-001   | tx-001        | 100    | 2022-10-10T10:10:00 | item-001 |
      | cust-001   | tx-002        | 200    | 2022-10-10T10:11:00 | item-002 |
      | cust-001   | tx-003        | 300    | 2022-10-10T10:12:00 | item-003 |
    When the customer transactions are aggregated
    Then the following records are generated
      | customerID | totalAmount | totalTransactions |
      | cust-001   | 600.0       | 3                 |

  Scenario: Multiple transactions for multiple customers
    Given the following transactions are received
      | customerID | transactionID | amount | txTime              | itemID   |
      | cust-001   | tx-001        | 100    | 2022-10-10T10:10:00 | item-001 |
      | cust-001   | tx-002        | 200    | 2022-10-10T10:11:00 | item-002 |
      | cust-002   | tx-003        | 150.50 | 2022-10-10T10:12:00 | item-003 |
      | cust-002   | tx-004        | 33.50  | 2022-10-10T10:13:00 | item-004 |
    When the customer transactions are aggregated
    Then the following records are generated
      | customerID | totalAmount | totalTransactions |
      | cust-001   | 300.0       | 2                 |
      | cust-002   | 184.0       | 2                 |

  Scenario: No transactions
    Given the following transactions are received
      | customerID | transactionID | amount | txTime | itemID |
    When the customer transactions are aggregated
    Then no records are generated

  Scenario: Corrupt records should be skipped
    Given the following transactions are received
      | customerID | transactionID | amount | txTime              | itemID   |
      | cust-001   | tx-001        | 100    | 2022-10-10T10:10:00 | item-001 |
      | cust-001   | tx-002        | ABC    | 2022-10-10T10:11:00 | item-002 |
      | cust-002   | tx-003        | 150.50 | 2022-10-10T10:12:00 | item-003 |
    When the customer transactions are aggregated
    Then the following records are generated
      | customerID | totalAmount | totalTransactions |
      | cust-001   | 100.0       | 1                 |
      | cust-002   | 150.50      | 1                 |
    And the following transactions are captured in error
      | customerID | transactionID | amount | txTime              | itemID   |
      | cust-001   | tx-002        | ABC    | 2022-10-10T10:11:00 | item-002 |


