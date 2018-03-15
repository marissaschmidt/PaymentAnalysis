use `payment`;
drop table if exists Result;

create table Result
  (AccountNumber varchar(10)
  ,OpenDate date
  ,ZipCode3 varchar(3) not null default '842'
  ,TotalCharges decimal(12,2)
  ,TotalAdjustments decimal(12,2)
  ,AdjustedTotalCharges decimal(12,2)
  ,TotalGoodStandingPayments30Day decimal(12,2)
  ,TotalGoodStandingPayments60Day decimal(12,2)
  ,TotalGoodStandingPayments90Day decimal(12,2)
  ,BadDebtTransferBalance decimal(12,2)
  ,TotalBadDebtPayments30Day decimal(12,2)
  ,TotalBadDebtPayments60Day decimal(12,2)
  ,TotalBadDebtPayments90Day decimal(12,2)
  ,PreviousAccountCount int
  ,PreviousAccountGoodStandingCharges decimal(12,2)
  ,PreviousAccountGoodStandingAdjustments decimal(12,2)
  ,PreviousAccountGoodStandingPayments decimal(12,2)
  ,PreviousAccountBadDebtPayments decimal(12,2)
  ,PRIMARY KEY(AccountNumber)
  );

insert
into Result
  (AccountNumber
  ,OpenDate
  ,ZipCode3
  )
select
   aa.AccountNumber
  ,aa.OpenDate
  ,ac.ZipCode3
from Account aa
  left join Customer ac
    on ac.CustomerNumber = aa.CustomerNumber;


update Result af 
  left join (
    select
      af.AccountNumber
      ,sum(at.TransactionAmount) TotalCharges
    from Result af
      join Transaction at
        on at.AccountNumber = af.AccountNumber
      where at.TransactionType = 'Charge'
      group by af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set af.TotalCharges = tt.TotalCharges;

update Result af 
  left join (
    select   
      af.AccountNumber
      ,sum(at.TransactionAmount) TotalAdjustments
    from Result af
      join Transaction at
        on at.AccountNumber = af.AccountNumber
    where at.TransactionType = 'Adjustment'
    group by af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set af.TotalAdjustments = tt.TotalAdjustments;

update Result af
set af.AdjustedTotalCharges = af.TotalCharges + af.TotalAdjustments;

update Result af
  left join (
    select   
      af.AccountNumber
      ,-(sum(case when datediff(at.TransactionDate, ash.StrategyStartDate) between 1 and 30 then at.TransactionAmount else 0 end)) TotalGoodStandingPayments30Day
      ,-(sum(case when datediff(at.TransactionDate, ash.StrategyStartDate) between 1 and 60 then at.TransactionAmount else 0 end)) TotalGoodStandingPayments60Day
      ,-(sum(case when datediff(at.TransactionDate, ash.StrategyStartDate) between 1 and 90 then at.TransactionAmount else 0 end)) TotalGoodStandingPayments90Day
    from Result af 
      join Transaction at
        on at.AccountNumber = af.AccountNumber
      join StrategyHistory ash
        on ash.AccountNumber = af.AccountNumber
          and ash.StrategyName = 'Good Standing'
      left join StrategyHistory ash2
        on ash2.AccountNumber = af.AccountNumber
          and ash2.StrategyName = 'Bad Debt'
    where at.TransactionType = 'Payment'
      and at.TransactionDate < ifnull(ash2.StrategyStartDate, '2099-12-31')
    group by af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set
   af.TotalGoodStandingPayments30Day = tt.TotalGoodStandingPayments30Day
  ,af.TotalGoodStandingPayments60Day = tt.TotalGoodStandingPayments60Day
  ,af.TotalGoodStandingPayments90Day = tt.TotalGoodStandingPayments90Day;

update Result af
  join( 
    select
      af.AccountNumber
      ,sum(at.TransactionAmount) BadDebtTransferBalance
    from Result af
      join Transaction at
        on at.AccountNumber = af.AccountNumber
      join StrategyHistory ash
        on ash.AccountNumber = af.AccountNumber
          and ash.StrategyName = 'Bad Debt'
    where at.TransactionDate <= ash.StrategyStartDate
    group by af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set
   af.BadDebtTransferBalance = tt.BadDebtTransferBalance;


update Result af
  join(
    select
      af.AccountNumber
      ,-(sum(case when datediff(at.TransactionDate, ash.StrategyStartDate) between 1 and 30 then at.TransactionAmount else 0 end)) TotalBadDebtPayments30Day
      ,-(sum(case when datediff(at.TransactionDate, ash.StrategyStartDate) between 1 and 60 then at.TransactionAmount else 0 end)) TotalBadDebtPayments60Day
      ,-(sum(case when datediff(at.TransactionDate, ash.StrategyStartDate) between 1 and 90 then at.TransactionAmount else 0 end)) TotalBadDebtPayments90Day
    from Result af
      join Transaction at
        on at.AccountNumber = af.AccountNumber
      join StrategyHistory ash
        on ash.AccountNumber = af.AccountNumber
          and ash.StrategyName = 'Bad Debt'
    where at.TransactionType = 'Payment'
    group by af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set
   af.TotalBadDebtPayments30Day = tt.TotalBadDebtPayments30Day
  ,af.TotalBadDebtPayments60Day = tt.TotalBadDebtPayments60Day
  ,af.TotalBadDebtPayments90Day = tt.TotalBadDebtPayments90Day;

update Result af
  join( 
    select
      af.AccountNumber
      ,count(1) PreviousAccountCount
    from Result af
      join Account aa1
        on aa1.AccountNumber = af.AccountNumber
      join Customer ac1
        on ac1.CustomerNumber = aa1.CustomerNumber
      join Customer ac2
        on ac2.Ssn = ac1.Ssn
      join Account aa2
        on aa2.CustomerNumber = ac2.CustomerNumber
          and aa2.AccountNumber != aa1.AccountNumber
          and aa2.OpenDate < aa1.OpenDate
    group by af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set
   af.PreviousAccountCount = tt.PreviousAccountCount;

update Result af
  join(
    select
      af.AccountNumber
      ,sum(case when at2.TransactionType = 'Charge' then at2.TransactionAmount else 0 end) PreviousAccountGoodStandingCharges
      ,sum(case when at2.TransactionType = 'Adjustment' then at2.TransactionAmount else 0 end) PreviousAccountGoodStandingAdjustments
      ,-(sum(case when at2.TransactionType = 'Payment' then at2.TransactionAmount else 0 end)) PreviousAccountGoodStandingPayments
    from Result af
      join Account aa1
        on aa1.AccountNumber = af.AccountNumber
      join Customer ac1
        on ac1.CustomerNumber = aa1.CustomerNumber
      join Customer ac2 
        on ac2.Ssn = ac1.Ssn
      join Account aa2
        on aa2.CustomerNumber = ac2.CustomerNumber
          and aa2.AccountNumber != aa1.AccountNumber
      join StrategyHistory ash2 
        on ash2.AccountNumber = aa2.AccountNumber
          and ash2.StrategyName = 'Good Standing'
      left join StrategyHistory ash2b
        on ash2b.AccountNumber = aa2.AccountNumber
          and ash2b.StrategyName = 'Bad Debt'
      join Transaction at2
        on at2.AccountNumber = aa2.AccountNumber
    where at2.TransactionType 
      in ('Charge' ,'Adjustment' ,'Payment')
      and at2.TransactionDate <= aa1.OpenDate
      and at2.TransactionDate >= ifnull(ash2.StrategyStartDate, '2099-12-31')
      and at2.TransactionDate < ifnull(ash2b.StrategyStartDate, '2099-12-31')
    group by 
      af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set
   af.PreviousAccountGoodStandingCharges = tt.PreviousAccountGoodStandingCharges
  ,af.PreviousAccountGoodStandingAdjustments = tt.PreviousAccountGoodStandingAdjustments
  ,af.PreviousAccountGoodStandingPayments = tt.PreviousAccountGoodStandingPayments;

update Result af
  join( 
    select
      af.AccountNumber
      ,-(sum(at2.TransactionAmount)) PreviousAccountBadDebtPayments
    from Result af
      join Account aa1
        on aa1.AccountNumber = af.AccountNumber
      join Customer ac1 
        on ac1.CustomerNumber = aa1.CustomerNumber
      join Customer ac2
        on ac2.Ssn = ac1.Ssn
      join Account aa2
        on aa2.CustomerNumber = ac2.CustomerNumber
          and aa2.AccountNumber != aa1.AccountNumber
      join StrategyHistory ash2
        on ash2.AccountNumber = aa2.AccountNumber
          and ash2.StrategyName = 'Bad Debt'
      join Transaction at2
        on at2.AccountNumber = aa2.AccountNumber
    where at2.TransactionType = 'Payment'
      and at2.TransactionDate <= aa1.OpenDate
      and at2.TransactionDate >= ifnull(ash2.StrategyStartDate, '2099-12-31')
    group by
       af.AccountNumber
  ) tt
    on tt.AccountNumber = af.AccountNumber
set
   af.PreviousAccountBadDebtPayments = tt.PreviousAccountBadDebtPayments;

