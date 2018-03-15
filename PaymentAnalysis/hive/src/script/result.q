DROP TABLE Result;

CREATE TABLE Result 
AS 
SELECT
  a.AccountNumber, 
  a.OpenDate, 
  c.ZipCode3,
  charges.TotalCharges,
  adjustments.TotalAdjustments,
  (charges.TotalCharges + adjustments.TotalAdjustments) AdjustedTotalCharges,
  goodPayments.TotalGoodStandingPayments30Day,
  goodPayments.TotalGoodStandingPayments60Day,
  goodPayments.TotalGoodStandingPayments90Day,
  transfer.BadDebtTransferBalance,
  badPayments.TotalBadDebtPayments30Day,
  badPayments.TotalBadDebtPayments60Day,
  badPayments.TotalBadDebtPayments90Day,
  previous.PreviousAccountCount,
  previousGoodTransactions.PreviousAccountGoodStandingCharges,
  previousGoodTransactions.PreviousAccountGoodStandingAdjustments,
  previousGoodTransactions.PreviousAccountGoodStandingPayments,
  previousBadTransactions.PreviousAccountBadDebtPayments


FROM 
  Account a 

    LEFT OUTER JOIN Customer c 
      ON (a.CustomerNumber = c.CustomerNumber) 


    LEFT OUTER JOIN 
      (SELECT 
        t.AccountNumber, 
        SUM(t.TransactionAmount) TotalCharges
      FROM Transaction t
      WHERE t.TransactionType = 'Charge' 
      GROUP BY t.AccountNumber
      ) charges  	
      ON (a.AccountNumber = charges.AccountNumber)


    LEFT OUTER JOIN 
      (SELECT
        t.AccountNumber,
        SUM(t.TransactionAmount) TotalAdjustments
      FROM Transaction t
      WHERE t.TransactionType = 'Adjustment'
      GROUP BY t.AccountNumber
      ) adjustments
      ON (a.AccountNumber = adjustments.AccountNumber)


    LEFT OUTER JOIN
      (SELECT 
        t.AccountNumber,
	-SUM( (case 
                 when (1 <= datediff(t.TransactionDate, sh.StrategyStartDate) 
                       AND datediff(t.TransactionDate, sh.StrategyStartDate) <= 30) 
                 then t.TransactionAmount else 0.0 
              end)
         ) TotalGoodStandingPayments30Day,
	-SUM( (case 
                 when (1 <= datediff(t.TransactionDate, sh.StrategyStartDate) 
                       AND datediff(t.TransactionDate, sh.StrategyStartDate) <= 60) 
                 then t.TransactionAmount else 0.0 
              end)
         ) TotalGoodStandingPayments60Day,
	-SUM( (case 
                 when (1 <= datediff(t.TransactionDate, sh.StrategyStartDate) 
                       AND datediff(t.TransactionDate, sh.StrategyStartDate) <= 90) 
                 then t.TransactionAmount else 0.0 
              end)
         ) TotalGoodStandingPayments90Day
	FROM Transaction t
        JOIN StrategyHistory sh
          ON (t.AccountNumber = sh.AccountNumber 
              AND sh.StrategyName = 'Good Standing')
        LEFT OUTER JOIN StrategyHistory sh2
          ON (t.AccountNumber = sh2.AccountNumber 
              AND sh2.StrategyName = 'Bad Debt')
       WHERE t.TransactionType = 'Payment'
              AND t.TransactionDate < if(sh2.StrategyStartDate IS NOT NULL, sh2.StrategyStartDate, '2099-12-31')
       GROUP BY t.AccountNumber
       ) goodPayments
       ON (a.AccountNumber = goodPayments.AccountNumber)


    LEFT OUTER JOIN
      (SELECT 
        t.AccountNumber,
        SUM(t.TransactionAmount) BadDebtTransferBalance
      FROM Transaction t
        JOIN StrategyHistory sh 
          ON (sh.AccountNumber = t.AccountNumber
              AND sh.StrategyName = 'Bad Debt')
      WHERE t.TransactionDate <= sh.StrategyStartDate
      GROUP BY t.AccountNumber
      ) transfer
      ON (transfer.AccountNumber = a.AccountNumber)


    LEFT OUTER JOIN
      (SELECT 
        t.AccountNumber,
	-SUM( (case 
                 when (1 <= datediff(t.TransactionDate, sh.StrategyStartDate) 
                       AND datediff(t.TransactionDate, sh.StrategyStartDate) <= 30) 
                 then t.TransactionAmount else 0.0 
              end)
         ) TotalBadDebtPayments30Day,
	-SUM( (case 
                 when (1 <= datediff(t.TransactionDate, sh.StrategyStartDate) 
                       AND datediff(t.TransactionDate, sh.StrategyStartDate) <= 60) 
                 then t.TransactionAmount else 0.0 
              end)
         ) TotalBadDebtPayments60Day,
	-SUM( (case 
                 when (1 <= datediff(t.TransactionDate, sh.StrategyStartDate) 
                       AND datediff(t.TransactionDate, sh.StrategyStartDate) <= 90) 
                 then t.TransactionAmount else 0.0 
              end)
         ) TotalBadDebtPayments90Day
	FROM Transaction t
        JOIN StrategyHistory sh
          ON (t.AccountNumber = sh.AccountNumber 
              AND sh.StrategyName = 'Bad Debt')
       WHERE t.TransactionType = 'Payment'
       GROUP BY t.AccountNumber
       ) badPayments
       ON (a.AccountNumber = badPayments.AccountNumber)


    LEFT OUTER JOIN 
      (SELECT 
         a1.AccountNumber, 
         COUNT(1) PreviousAccountCount
       FROM Account a1
         JOIN Customer c1 
           ON (c1.CustomerNumber = a1.CustomerNumber)
         JOIN Customer c2
           ON (c2.Ssn = c1.Ssn)
         JOIN Account a2
           ON (a2.CustomerNumber = c2.CustomerNumber)
       WHERE a2.AccountNumber <> a1.AccountNumber 
         AND a2.OpenDate < a1.OpenDate
       GROUP BY a1.AccountNumber
      ) previous
      ON (a.AccountNumber = previous.AccountNumber)


    LEFT OUTER JOIN
      (SELECT 
        a1.AccountNumber,
        SUM((case 
                 when t2.TransactionType = 'Charge' 
                 then t2.TransactionAmount else 0.0 
              end)
        ) PreviousAccountGoodStandingCharges,
        SUM((case 
                 when t2.TransactionType = 'Adjustment' 
                 then t2.TransactionAmount else 0.0 
              end)
        ) PreviousAccountGoodStandingAdjustments,
        (-SUM((case 
                 when t2.TransactionType = 'Payment' 
                 then t2.TransactionAmount else 0.0 
              end)
         )) PreviousAccountGoodStandingPayments
      FROM Account a1
        JOIN Customer c1
          ON (c1.CustomerNumber = a1.CustomerNumber)
        JOIN Customer c2
          ON (c2.Ssn = c1.Ssn)
        JOIN Account a2
          ON (a2.CustomerNumber = c2.CustomerNumber)
        JOIN StrategyHistory sh2
          ON (sh2.AccountNumber = a2.AccountNumber AND
             sh2.StrategyName = 'Good Standing')
        LEFT OUTER JOIN StrategyHistory sh2b
          ON (sh2b.AccountNumber = a2.AccountNumber AND
              sh2b.StrategyName = 'Bad Debt')
        JOIN Transaction t2
          ON (t2.AccountNumber = a2.AccountNumber)
      WHERE a2.AccountNumber <> a1.AccountNumber
        AND datediff(t2.TransactionDate, a1.OpenDate) <= 0
        AND datediff(t2.TransactionDate, if(sh2.StrategyStartDate IS NOT NULL, sh2.StrategyStartDate, '2099-12-31')) >= 0
        AND datediff(t2.TransactionDate, if(sh2b.StrategyStartDate IS NOT NULL, sh2b.StrategyStartDate, '2099-12-31')) < 0
      GROUP BY a1.AccountNumber
      ) previousGoodTransactions
      ON (previousGoodTransactions.AccountNumber = a.AccountNumber)


    LEFT OUTER JOIN
      (SELECT 
        a1.AccountNumber,
        -(SUM(t2.TransactionAmount)) PreviousAccountBadDebtPayments
      FROM Account a1
        JOIN Customer c1
          ON (c1.CustomerNumber = a1.CustomerNumber)
        JOIN Customer c2 
          ON (c2.Ssn = c1.Ssn)
        JOIN Account a2 
          ON (a2.CustomerNumber = c2.CustomerNumber)
        JOIN StrategyHistory sh2
          ON (sh2.AccountNumber = a2.AccountNumber AND
             sh2.StrategyName = 'Bad Debt')   
        JOIN Transaction t2
          ON (t2.AccountNumber = a2.AccountNumber)
      WHERE a2.AccountNumber <> a1.AccountNumber
        AND t2.TransactionType = 'Payment'
        AND datediff(t2.TransactionDate, a1.OpenDate) <= 0
        AND datediff(t2.TransactionDate, if(sh2.StrategyStartDate IS NOT NULL, sh2.StrategyStartDate, '2099-12-31')) >= 0
      GROUP BY a1.AccountNumber
      ) previousBadTransactions
        ON (previousBadTransactions.AccountNumber = a.AccountNumber)
;
