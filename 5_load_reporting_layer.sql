-- Reporting table - Branch Summary
create database rptdb_bfsi;

CREATE TABLE rptdb_bfsi.rpt_branch_summary AS
SELECT 
    b.BranchID,
    b.BranchName,
    COUNT(DISTINCT c.CustomerID) AS Total_Customers,
    COUNT(DISTINCT a.AccountID) AS Total_Accounts,
    SUM(a.Balance) AS Total_Balance,
    SUM(p.Amount) AS Total_Payments,
    SUM(t.Amount) AS Total_Transactions
FROM edwdb_bfsi.dim_branches b
LEFT JOIN edwdb_bfsi.dim_customers c ON c.BranchID = b.BranchID
LEFT JOIN odsdb_bfsi.ods_accounts a ON a.CustomerID = c.CustomerID
LEFT JOIN bfsi_payment_mart.fact_payments p ON p.FromAccountID = a.AccountID
LEFT JOIN bfsi_trans_mart.fact_transactions t ON t.AccountID = a.AccountID
GROUP BY b.BranchID, b.BranchName;


CREATE VIEW rptdb_bfsi.vw_top_performing_branches AS
SELECT 
    BranchID,
    BranchName,
    Total_Balance,
    Total_Payments,
    Total_Transactions,
    RANK() OVER (ORDER BY Total_Balance DESC) AS Balance_Rank,
    RANK() OVER (ORDER BY Total_Payments DESC) AS Payment_Rank
FROM rptdb_bfsi.rpt_branch_summary;


CREATE TABLE rptdb_bfsi.rpt_suspicious_transactions AS
SELECT * FROM bfsi_trans_mart.fact_transactions WHERE Suspicious = TRUE;

CREATE VIEW rptdb_bfsi.vw_branch_summary_stats AS
SELECT 
    BranchID,
    BranchName,
    Total_Customers,
    Total_Accounts,
    Total_Balance,
    Total_Payments,
    Total_Transactions,
    ROUND(Total_Balance / NULLIF(Total_Accounts, 0), 2) AS Avg_Balance_Per_Account,
    ROUND(Total_Payments / NULLIF(Total_Customers, 0), 2) AS Avg_Payment_Per_Customer
FROM rptdb_bfsi.rpt_branch_summary;


CREATE VIEW rptdb_bfsi.vw_suspicious_transactions_details AS
SELECT 
    t.TransactionID,
    t.AccountID,
    a.CustomerID,
    c.firstname,
    c.BranchID,
    b.BranchName,
    t.TransactionDate,
    t.Amount,
    t.TransactionType,
    t.Suspicious
FROM rptdb_bfsi.rpt_suspicious_transactions t
LEFT JOIN odsdb_bfsi.ods_accounts a ON t.AccountID = a.AccountID
LEFT JOIN edwdb_bfsi.dim_customers c ON a.CustomerID = c.CustomerID
LEFT JOIN edwdb_bfsi.dim_branches b ON c.BranchID = b.BranchID;