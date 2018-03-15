package mapred.reduce;

import io.AccountStatisticsWritable;
import io.AccountWritable;
import io.StrategyType;
import io.TextDatePair;
import io.TransactionType;
import io.TransactionWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class PastAccountStatisticsReducerWithObjects extends MapReduceBase 
	implements Reducer<TextDatePair, TransactionWritable, Text, AccountWritable> 
{
    private Text outKey;
    private AccountStatisticsWritable accountStatistics;
    private AccountWritable outValue;

    @Override
    public void reduce(TextDatePair key, Iterator<TransactionWritable> values, OutputCollector<Text, AccountWritable> output, Reporter reporter)
	throws IOException 
    {
	int previousAccounts = 0;
	double[] totalGoodTransactions = new double[TransactionType.NUM_TYPES];
	double totalBadPayments = 0.0;

	while (values.hasNext()) 
	{
	    TransactionWritable transaction = values.next();
	    TransactionType transactionType = transaction.getTransactionType();
	    StrategyType strategyType = transaction.getStrategyType();
	    
	    if(transactionType.equals(TransactionType.UNKNOWN) && strategyType.equals(StrategyType.UNKNOWN)) // it is account info
	    {
		outKey = new Text(transaction.getUniqueId());
		accountStatistics = new AccountStatisticsWritable();
		accountStatistics.setPreviousAccountCount(previousAccounts);
		accountStatistics.setPreviousAccountGoodStandingCharges(totalGoodTransactions[TransactionType.CHARGE.getOffset()]);
		accountStatistics.setPreviousAccountGoodStandingAdjustments(totalGoodTransactions[TransactionType.ADJUSTMENT.getOffset()]);
		accountStatistics.setPreviousAccountGoodStandingPayments(totalGoodTransactions[TransactionType.PAYMENT.getOffset()]);
		accountStatistics.setPreviousAccountBadDebtPayments(totalBadPayments);
		outValue = new AccountWritable(accountStatistics);
		output.collect(outKey, outValue);
		    
		// increment number of previous accounts
		previousAccounts++;		
	    }
	    else // lets add it up!
	    {
		double amount = transaction.getAmount();
		
		if(strategyType.isGoodStrategy())
		{
		    totalGoodTransactions[transactionType.getOffset()] += amount;
		}
		else 
		{
		    totalBadPayments += amount;
		}
	    }
	}
    }
}
