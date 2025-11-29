-- ODS loads from staging with exact copy
-- ODS Load from Staging with load_dt and load_ts

INSERT
	INTO
	odsdb_bfsi.ods_accounts
SELECT
	AccountID,
	trim(AccountType),
	Balance,
	CreditScore,
	upper(Currency),
	CustomerID,
	DateOpened,
	ManagerID,
	ODLimit,
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_accounts
where
	AccountID is not null;

INSERT
	INTO
	odsdb_bfsi.ods_transactions
SELECT
	t.*,
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_transactions t;

INSERT
	INTO
	odsdb_bfsi.ods_payments
SELECT
	p.*,
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_payments p;

INSERT
	INTO
	odsdb_bfsi.ods_creditcard
SELECT
	c.*,
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_creditcard c;

INSERT
	INTO
	odsdb_bfsi.ods_loans
SELECT
	l.*,
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_loans l;

INSERT
	INTO
	odsdb_bfsi.ods_cust_profile
SELECT
	Address,
	BranchID,
	CustomerID,
	DateOfBirth,
	Email,
	FirstName,
	LastName,
	substr(PhoneNumber, 1, 20),
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_cust_profile cp;

INSERT
	INTO
	odsdb_bfsi.ods_branches
SELECT
	b.*,
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_branches b;

INSERT
	INTO
	odsdb_bfsi.ods_employees
SELECT
	e.*,
	CURRENT_DATE AS load_dt,
	CURRENT_TIMESTAMP AS load_ts
FROM
	stgdb.stg_employees e;