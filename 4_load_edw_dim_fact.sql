-- History is not maintained SCD T1
truncate table edwdb_bfsi.dim_customers;
INSERT INTO edwdb_bfsi.dim_customers (
    CustomerID,
    FirstName,
    LastName,
    Email,
    PhoneNumber,
    Address,
    DateOfBirth,
    BranchID,
    effective_date
)
SELECT DISTINCT
    CustomerID,
    FirstName,
    LastName,
    Email,
    PhoneNumber,
    Address,
    DateOfBirth,
    BranchID,
    CURRENT_DATE
FROM odsdb_bfsi.ods_cust_profile;

-- History is maintained SCD T2 starts here
SET SQL_SAFE_UPDATES = 0;

UPDATE edwdb_bfsi.dim_branches d
JOIN odsdb_bfsi.ods_branches o
  ON d.BranchID = o.BranchID
SET d.end_date = CURRENT_DATE - INTERVAL 1 DAY,
    d.is_current = 0
WHERE d.is_current = 1
  AND d.BranchID IS NOT NULL  -- ensures key column is used
  AND (d.Address   <> o.Address
    OR d.BranchName <> o.BranchName
    OR d.City       <> o.City
    OR d.State      <> o.State
    OR d.Zipcode    <> o.Zipcode);

INSERT INTO edwdb_bfsi.dim_branches (
    Address,
    BranchID,
    BranchName,
    City,
    State,
    Zipcode,
    start_date,
    end_date,
    is_current
)
SELECT
    o.Address,
    o.BranchID,
    o.BranchName,
    o.City,
    o.State,
    o.Zipcode,
    CURRENT_DATE,
    NULL,
    1
FROM odsdb_bfsi.ods_branches o
LEFT JOIN edwdb_bfsi.dim_branches d
       ON o.BranchID = d.BranchID
      AND d.is_current = 1
WHERE d.BranchID IS NULL
   OR d.Address   <> o.Address
   OR d.BranchName <> o.BranchName
   OR d.City       <> o.City
   OR d.State      <> o.State
   OR d.Zipcode    <> o.Zipcode;

-- History is maintained SCD T2 ends here

INSERT INTO edwdb_bfsi.dim_employees (
    BranchID,
    EmployeeID,
    FirstName,
    Hiredate,
    LastName,
    ManagerID,
    Position
)
SELECT
    BranchID,
    EmployeeID,
    FirstName,
    Hiredate,
    LastName,
    ManagerID,
    Position
FROM odsdb_bfsi.ods_employees;

INSERT INTO edwdb_bfsi.dim_loans (
    Amount,
    Collateral,
    CustomerID,
    EndDate,
    InterestRate,
    LoanID,
    LoanType,
    PaymentFrequency,
    StartDate,
    Status
)
SELECT
    Amount,
    Collateral,
    CustomerID,
    EndDate,
    InterestRate,
    LoanID,
    LoanType,
    PaymentFrequency,
    StartDate,
    Status
FROM odsdb_bfsi.ods_loans;

INSERT INTO edwdb_bfsi.fact_loans (
    LoanID,
    CustomerID,
    BranchID,
    Amount,
    InterestRate,
    StartDate,
    EndDate,
    PaymentFrequency,
    Status,
    OutstandingBalance,
    LoanDurationMonths,
    RiskIndicator,
    HighValueFlag,
    load_dt,
    load_ts
)
SELECT distinct
    o.LoanID,
    c.CustomerID,          -- from dim_customers
    b.BranchID,            -- from dim_branches
    o.Amount,
    o.InterestRate,
    o.StartDate,
    o.EndDate,
    o.PaymentFrequency,
    o.Status,
    o.Amount AS OutstandingBalance,
    TIMESTAMPDIFF(MONTH, o.StartDate, o.EndDate) AS LoanDurationMonths,
    CASE 
        WHEN o.InterestRate > 12 THEN 'HIGH'
        WHEN o.InterestRate BETWEEN 8 AND 12 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS RiskIndicator,

    CASE 
        WHEN o.Amount >= 1000000 THEN 'Y'
        ELSE 'N'
    END AS HighValueFlag,
    CURRENT_DATE,
    CURRENT_TIMESTAMP
FROM odsdb_bfsi.ods_loans o
LEFT JOIN edwdb_bfsi.dim_customers c
    ON o.CustomerID = c.CustomerID
LEFT JOIN edwdb_bfsi.dim_branches b
    ON c.BranchID = b.BranchID
   AND b.is_current = 1;

-- Fact Transactions with transaction_flag derivation
INSERT INTO bfsi_trans_mart.fact_transactions (
    AccountID,
    Amount,
    Currency,
    Description,
    EventTs,
    Status,
    Suspicious,
    TransactionDate,
    TransactionFee,
    TransactionID,
    TransactionType,
    transaction_flag
)
SELECT
    AccountID,
    Amount,
    Currency,
    Description,
    EventTs,
    Status,
    Suspicious,
    TransactionDate,
    TransactionFee,
    TransactionID,
    TransactionType,
    CASE WHEN Suspicious THEN 'FLAGGED' ELSE 'NORMAL' END
FROM odsdb_bfsi.ods_transactions;


-- Fact Payments with derived AmountInBaseCurrency
INSERT INTO bfsi_payment_mart.fact_payments (
    Amount,
    AuditTrial,
    ClearingSystem,
    Currency,
    CustomerSegment,
    Description,
    ExchangeRate,
    Fee,
    FromAccountID,
    MerchantName,
    PaymentDate,
    PaymentID,
    PaymentType,
    ToAccountID,
    AmountInBaseCurrency
)
SELECT
    Amount,
    AuditTrial,
    ClearingSystem,
    Currency,
    CustomerSegment,
    Description,
    ExchangeRate,
    Fee,
    FromAccountID,
    MerchantName,
    PaymentDate,
    PaymentID,
    PaymentType,
    ToAccountID,
    Amount * ExchangeRate
FROM odsdb_bfsi.ods_payments;

-- Star Schema Model: Fact Creditcard with UtilizationPercent
INSERT INTO bfsi_cc_mart.fact_creditcard (
    customerid,
    loanid,
    employeeid,
    firstname,
    phonenumber,
    cardid,
    cardtype,
    balance,
    creditlimit,
    billcycle,
    issuedate,
    utilization_percent,
    load_dt,
    load_ts
)
SELECT
    oc.customerid,
    dl.loanid,
    de.employeeid,
    dcu.firstname,
    dcu.phonenumber,
    oc.cardid,
    oc.cardtype,
    oc.balance,
    oc.creditlimit,
    oc.billcycle,
    oc.issuedate,
    ROUND((oc.balance / oc.creditlimit) * 100, 2) AS utilization_percent,
    oc.load_dt,
    oc.load_ts
FROM odsdb_bfsi.ods_creditcard oc
LEFT JOIN edwdb_bfsi.dim_customers dcu 
    ON oc.customerid = dcu.customerid
LEFT JOIN edwdb_bfsi.dim_loans dl 
    ON oc.customerid = dl.customerid
LEFT JOIN edwdb_bfsi.dim_employees de 
    ON dcu.branchid = de.branchid
WHERE oc.load_dt = (
        SELECT MAX(load_dt) 
        FROM odsdb_bfsi.ods_creditcard
    );