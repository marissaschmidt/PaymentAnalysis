package mapred.reduce;

import io.AccountStatisticsWritable;
import io.AccountWritable;
import io.DateWritable;
import io.StrategyType;
import io.TextDatePair;
import io.TransactionType;
import io.TransactionWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

@SuppressWarnings("deprecation")
public class AccountStatisticsReducerWithObjects extends MapReduceBase
	implements Reducer<TextDatePair, TransactionWritable, Text, AccountWritable> 
{
    private final double[][] transactions = new double[TransactionType.NUM_TYPES][StrategyType.NUM_TYPES];
    MultipleOutputs mos;
    
    private Text keyForAccount;
    private TextDatePair keyForTransaction;
    private AccountStatisticsWritable accountStatistics;
    private AccountWritable valueForAccount;
    private TransactionWritable valueForTransaction;
    
    @SuppressWarnings("unchecked")
    @Override
    public void reduce(TextDatePair key, Iterator<TransactionWritable> values, 
	    OutputCollector<Text, AccountWritable> output, Reporter reporter) throws IOException 
    {
	Text accountNumber = key.getFirst();
	
	// Account transaction should be first
	TransactionWritable customerInfo = values.next();
	String ssn = customerInfo.getUniqueId().toString();
	Date openDate = customerInfo.getDate();
	
	// First, write a transaction for the account open date
	keyForTransaction = new TextDatePair(ssn, openDate);
	valueForTransaction = new TransactionWritable(accountNumber);
	mos.getCollector("TransactionWritable", reporter).collect(keyForTransaction, valueForTransaction);

	// Then go through transactions
	StrategyType currentStrategy = StrategyType.UNKNOWN;
	while (values.hasNext()) 
	{
	    TransactionWritable transaction = values.next();
	    StrategyType strategyType = transaction.getStrategyType();
	    
	    if(strategyType.equals(StrategyType.UNKNOWN)) // It is a transaction
	    {
		TransactionType transactionType = transaction.getTransactionType();
		if(transactionType.equals(TransactionType.UNKNOWN))
		{
		    //There is an error
		}
		else
		{
		    Date date = transaction.getDate();
		    double amount = transaction.getAmount();
		    
		    transactions[transactionType.getOffset()][currentStrategy.getOffset()] += amount;
		    
		    // write all payments and good charges and adjustments
		    if(transactionType.equals(TransactionType.PAYMENT) || strategyType.isGoodStrategy())
		    {
			keyForTransaction = new TextDatePair(ssn, date);
			valueForTransaction = new TransactionWritable(accountNumber, transactionType, strategyType, new DateWritable(date), new DoubleWritable(amount));
			mos.getCollector("TransactionWritable", reporter).collect(keyForTransaction, valueForTransaction);
		    }
		}
	    }
	    else // Change the strategy to the current one 
	    {
		if(!(currentStrategy.equals(StrategyType.UNKNOWN) || strategyType.equals(StrategyType.BAD_DEBT)))
		{
		    for(double[] arrayForType : transactions)
		    {
			arrayForType[strategyType.getOffset()] = arrayForType[currentStrategy.getOffset()];
		    }
		}
		currentStrategy = strategyType;
	    }
	}
	
	accountStatistics.setGoodStandingAdjustments(transactions[TransactionType.ADJUSTMENT.getOffset()][StrategyType.GOOD_STRATEGY_NINETY.getOffset()]);
	accountStatistics.setGoodStandingCharges(transactions[TransactionType.CHARGE.getOffset()][StrategyType.GOOD_STRATEGY_NINETY.getOffset()]);
	accountStatistics.setGoodStandingPayments30Day(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.GOOD_STRATEGY.getOffset()]);
	accountStatistics.setGoodStandingPayments60Day(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.GOOD_STRATEGY_THIRTY.getOffset()]);
	accountStatistics.setGoodStandingPayments90Day(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.GOOD_STRATEGY_SIXTY.getOffset()]);
	accountStatistics.setGoodStandingPaymentsOther(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.GOOD_STRATEGY_NINETY.getOffset()]);
	
	accountStatistics.setBadDebtAdjustments(transactions[TransactionType.ADJUSTMENT.getOffset()][StrategyType.BAD_DEBT_NINETY.getOffset()]);
	accountStatistics.setBadDebtCharges(transactions[TransactionType.CHARGE.getOffset()][StrategyType.BAD_DEBT_NINETY.getOffset()]);
	accountStatistics.setBadDebtPayments30Day(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.BAD_DEBT.getOffset()]);
	accountStatistics.setBadDebtPayments60Day(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.BAD_DEBT_THIRTY.getOffset()]);
	accountStatistics.setBadDebtPayments90Day(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.BAD_DEBT_SIXTY.getOffset()]);
	accountStatistics.setBadDebtPaymentsOther(-transactions[TransactionType.PAYMENT.getOffset()][StrategyType.BAD_DEBT_NINETY.getOffset()]);
	
	keyForAccount = new Text(accountNumber);
	valueForAccount = new AccountWritable(accountStatistics);
	mos.getCollector("AccountWritable", reporter).collect(keyForAccount, valueForAccount);
    }
    
    @Override
    public void configure(JobConf job) 
    {
	for(double[] arrayForType : transactions)
	{
	    Arrays.fill(arrayForType, 0);
	}
    	mos = new MultipleOutputs(job);
    }
    
    @Override
    public void close() throws IOException 
    {
    	mos.close();
    }
}