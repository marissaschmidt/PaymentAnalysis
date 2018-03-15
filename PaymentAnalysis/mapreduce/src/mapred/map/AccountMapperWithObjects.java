package mapred.map;

import io.AccountWritable;
import io.DateWritable;
import io.InvalidDateException;
import io.TextPair;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class AccountMapperWithObjects extends MapReduceBase implements
	Mapper<Object, Text, TextPair, AccountWritable>
{
    static enum Counters
    {
	INVALID_ACCOUNT
    }

    public static final int COL_ACCOUNT_NUM  = 0;
    public static final int COL_OPEN_DATE    = 1;
    public static final int COL_CUSTOMER_NUM = 2;

    private static final IntWritable zipCode3 = new IntWritable(0);
    private static final Text ssn = new Text("");
    private static final Text ONE = new Text("1");

    private TextPair key = new TextPair();
    private AccountWritable value;

    /**
     * Processes each Account entity and outputs a new {@link AccountWritable}
     * object with the {@code accountNumber}, {@code openDate}, and
     * {@code customerNumber} fields set to the values from the current entity
     * and the {@code zipCode3} and {@code ssn} fields set to 0 and {@code ""},
     * respectively.
     * <p>
     * key: [CustomerNumber,1] <br>
     * value:
     * {@code new AccountWritable(CustomerNumber, AccountNumber, "", OpenDate, 0)}
     * </p>
     */
    @Override
    public void map(Object offset, Text input,
	    OutputCollector<TextPair, AccountWritable> output,
	    Reporter reporter) throws IOException
    {
	String[] parts = (input.toString()).split(",");

	DateWritable openDate;
	try
	{
	    openDate = new DateWritable(parts[COL_OPEN_DATE].trim());
	} 
	catch (InvalidDateException e)
	{
	    reporter.incrCounter(Counters.INVALID_ACCOUNT, 1);
	    return;
	}

	Text accountNumber = new Text(parts[COL_ACCOUNT_NUM].trim());
	Text customerNumber = new Text(parts[COL_CUSTOMER_NUM].trim());

	key.set(customerNumber, ONE);
	value = new AccountWritable(ssn, customerNumber, accountNumber,openDate, zipCode3);
	output.collect(key, value);
    }
}
