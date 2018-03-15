
drop schema if exists `payment`;
CREATE SCHEMA `payment`;
use `payment`;

CREATE TABLE Customer (
    CustomerNumber varchar(10),
    FirstName      varchar(25),
    LastName       varchar(25),
    Ssn            varchar(10),
    ZipCode3       varchar(3),
    PRIMARY KEY (CustomerNumber)
);
CREATE TABLE Account (
    AccountNumber  varchar(10),
    OpenDate       date,
    CustomerNumber varchar(10),
    PRIMARY KEY (AccountNumber),
    FOREIGN KEY (CustomerNumber) REFERENCES Customer(CustomerNumber)
);
CREATE TABLE StrategyHistory (
    AccountNumber     varchar(10),
    StrategyName      varchar(15),
    StrategyStartDate date,
    PRIMARY KEY (AccountNumber, StrategyName),
    FOREIGN KEY (AccountNumber) REFERENCES Account(AccountNumber)
);
CREATE TABLE Transaction(
    AccountNumber     varchar(10),
    TransactionDate   date,
    TransactionAmount decimal(12,2),
    TransactionType   varchar(10),
    PRIMARY KEY (AccountNumber, TransactionDate, 
                     TransactionAmount, TransactionType),
    FOREIGN KEY (AccountNumber) REFERENCES Account(AccountNumber)
);



