package mapred.map;

import io.DateWritable;
import io.InvalidDateException;
import io.StrategyType;
import io.TextDatePair;
import io.TransactionWritable;
import io.TransactionType;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [AccountNumber,date] value: [type, amount, date]
 */
@SuppressWarnings("deprecation")
public class TransactionMapperWithObjects extends MapReduceBase implements
		Mapper<Object, Text, TextDatePair, TransactionWritable>
{
    static enum Counters 
    {
	INVALID_TRANSACTION
    }

    public static final int COL_ACCOUNT_ID = 0;
    public static final int COL_DATE       = 1;
    public static final int COL_AMOUNT     = 2;
    public static final int COL_TYPE       = 3;
    
    private static final StrategyType strategyType = StrategyType.UNKNOWN;
    
    private TextDatePair key = new TextDatePair();
    private TransactionWritable	value;
    
    /**
     * Processes each Transaction entity and outputs a new {@link TransactionWritable}
     * object with the {@code transactionType}, {@code date}, and {@code amount} fields 
     * set to the values from the current entity and the {@code strategyType} field set 
     * to {@link StrategyType#UNKNOWN}.
     * <p>
     * key: [AccountNumber, TransactionDate] 
     * <br>
     * value: {@code new TransactionWritable(TransactionType, StrategyType.UNKOWN, TransactionDate, TransactionAmount)}
     * </p>
     */
    @Override
    public void map(Object offset, Text input, 
	    OutputCollector<TextDatePair, TransactionWritable> output,
	    Reporter reporter) throws IOException
    {
	String[] parts = input.toString().split(",");

	if (parts.length > 4)
	{
	    parts[2] = (parts[2] + parts[3]).replaceAll("[ \"]", "");
	    parts[3] = parts[4];
	}

	Text accountNumber = new Text(parts[COL_ACCOUNT_ID].trim());
	DateWritable date;
	try
	{
	    date = new DateWritable(parts[COL_DATE].trim());
	} 
	catch (InvalidDateException e)
	{
	    reporter.incrCounter(Counters.INVALID_TRANSACTION, 1);
	    return;
	}
	DoubleWritable amount = new DoubleWritable(Double.parseDouble(parts[COL_AMOUNT].trim()));
	TransactionType transactionType = TransactionType.getByName(parts[COL_TYPE].trim());

	key.set(accountNumber, date);
	value = new TransactionWritable(accountNumber, transactionType, strategyType, date, amount);

	output.collect(key, value);
    }
}