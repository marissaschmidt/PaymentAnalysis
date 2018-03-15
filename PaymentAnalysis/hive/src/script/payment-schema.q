CREATE TABLE Customer (
	CustomerNumber STRING, 
	FirstName STRING,
	LastName STRING,
	Ssn STRING,
	ZipCode3 STRING)
CLUSTERED BY (CustomerNumber) INTO 4 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

CREATE TABLE Account (
	AccountNumber STRING,
	OpenDate STRING,
	CustomerNumber STRING)
CLUSTERED BY (AccountNumber) INTO 4 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

CREATE TABLE StrategyHistory (
	AccountNumber STRING,
	StrategyName STRING,
	StrategyStartDate STRING)
CLUSTERED BY (AccountNumber) INTO 4 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

CREATE TABLE Transaction (
	AccountNumber STRING,
	TransactionDate STRING,
	TransactionAmount DOUBLE,
	TransactionType STRING)
CLUSTERED BY (AccountNumber) INTO 4 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';