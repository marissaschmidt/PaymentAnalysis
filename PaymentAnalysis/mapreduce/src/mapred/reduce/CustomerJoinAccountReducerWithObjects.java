package mapred.reduce;

import io.AccountWritable;
import io.DateWritable;
import io.StrategyType;
import io.TextDatePair;
import io.TextPair;
import io.TransactionType;
import io.TransactionWritable;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;


@SuppressWarnings("deprecation")
public class CustomerJoinAccountReducerWithObjects extends MapReduceBase
		implements Reducer<TextPair, AccountWritable, TextDatePair, AccountWritable>
{
    MultipleOutputs mos;
    
    private static final TransactionType transactionType = TransactionType.UNKNOWN;
    private static final StrategyType strategyType = StrategyType.UNKNOWN;
    private static final DoubleWritable amount = new DoubleWritable(0.0);
    
    private Text keyForAccount = new Text();
    private TextDatePair keyForTransaction = new TextDatePair();
    private AccountWritable valueForAccount;
    private TransactionWritable valueForTransaction;
 
    /**
     * Joins the Account and Customer relations on the {@code CustomerNumber}.
     * <p>
     * and outputs a new {@link AccountWritable}
     * object with the {@code customerNumber}, {@code zipCode3}, and 
     * {@code ssn} fields set to the values from the current entity 
     * and the {@code accountNumber} and {@code openDate} fields set to {@code ""}.
     * </p>
     * <p>
     * key: [AccountNumber,1] 
     * <br>
     * value: [Ssn, OpenDate, ZipCode3]
     * <br>
     * key: [CustomerNumber,0] (since OpenDate is 0, this will always be first)
     * <br>
     * value: {@code new AccountWritable(CustomerNumber, "", Ssn, "", ZipCode3)}
     * </p>
     */
    @SuppressWarnings("unchecked")
    @Override
    public void reduce(TextPair key, Iterator<AccountWritable> values, OutputCollector<TextDatePair, AccountWritable> output, Reporter reporter) throws IOException
    {
	// The first field of the key is the customerNumber
	Text customerNumber = key.getFirst();
	
	// Customer will be first because the secondary key is ZERO.
	AccountWritable customer = values.next();
	IntWritable zipCode3 = new IntWritable(customer.getZipCode3());
	Text ssn = new Text(customer.getSsn());

	while (values.hasNext())
	{
	    AccountWritable account = values.next();
	    
	    Text accountNumber = new Text(account.getAccountNumber());
	    DateWritable openDate = new DateWritable(account.getOpenDate());
	    openDate.add(Calendar.DAY_OF_MONTH, -1);

	    keyForAccount.set(accountNumber);
	    valueForAccount = new AccountWritable(ssn, customerNumber, accountNumber, openDate, zipCode3);
	    mos.getCollector("AccountWritable", reporter).collect(keyForAccount, valueForAccount);
	    
	    keyForTransaction.set(accountNumber, openDate);
	    valueForTransaction = new TransactionWritable(ssn, transactionType, strategyType, openDate, amount);
	    mos.getCollector("TransactionWritable", reporter).collect(keyForTransaction, valueForTransaction);
	}
    }
    
    @Override
    public void configure(JobConf job) 
    {
	mos = new MultipleOutputs(job);
    }
    
    @Override
    public void close() throws IOException 
    {
	mos.close();
    }
}