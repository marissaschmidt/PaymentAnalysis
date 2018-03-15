package mapred.map;

import io.DateWritable;
import io.InvalidDateException;
import io.StrategyType;
import io.TextDatePair;
import io.TransactionType;
import io.TransactionWritable;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [AccountNumber,date] value: [GoodStanding]
 * 
 * key: [AccountNumber,date] value: [BadDebt]
 */
@SuppressWarnings("deprecation")
public class StrategyHistoryMapperWithObjects extends MapReduceBase implements
		Mapper<Object, Text, TextDatePair, TransactionWritable> 
{
    static enum Counters
    {
	INVALID_STRATEGY
    }
	
    public static final int COL_ACCOUNT_NUMBER = 0;
    public static final int COL_NAME = 1;
    public static final int COL_DATE = 2;

    private static final DoubleWritable amount = new DoubleWritable(0.0);
    private static final TransactionType transactionType = TransactionType.UNKNOWN;
    
    private TextDatePair key = new TextDatePair();
    private TransactionWritable value;

    /**
     * Processes each StrategyHistory entity and outputs a new {@link TransactionWritable}
     * object for each period for the strategy (30 day, 60 day, and 90 day) with the 
     * {@code strategyType} and {@code startDate} fields set to the values from the 
     * current entity and the {@code transactionType} and {@code amount} fields set 
     * to {@link TransactionType#UNKNOWN} and 0, respectively.
     * <p>
     * key: [AccountNumber, StartDate] 
     * <br>
     * value: {@code TransactionWritable(TransactionType.UNKNOWN, StrategyType, StartDate, 0)}
     * </p>
     */
    @Override
    public void map(Object offset, Text input, OutputCollector<TextDatePair, TransactionWritable> output, 
	    Reporter reporter) throws IOException 
    {
	String[] parts = (input.toString()).split(",");

	Text accountNumber = new Text(parts[COL_ACCOUNT_NUMBER].trim());
	StrategyType strategyType = StrategyType.getByName(parts[COL_NAME].trim());
	DateWritable startDate = null;
	try
	{
	    startDate = new DateWritable(parts[COL_DATE].trim());
	} 
	catch (InvalidDateException e)
	{
	    reporter.incrCounter(Counters.INVALID_STRATEGY, 1);
	    return;
	}
		
	key.set(accountNumber, startDate);
	value = new TransactionWritable(accountNumber, transactionType, strategyType, startDate, amount);
	output.collect(key, value);

	// While we are processing this strategy, we will output transactions 
	// for the 30, 60, and 90 day periods of this strategy.
	do 
	{
	    strategyType = StrategyType.getNextStrategy(strategyType);
	    
	    key.set(accountNumber, DateWritable.getAdjustedDate(startDate, Calendar.DAY_OF_MONTH, strategyType.getNumberOfDays()));
	    value.setStrategyType(strategyType);
	    output.collect(key, value);
	} 
	while (strategyType.getNumberOfDays() != StrategyType.MAX_STRATEGY_VALUE);
    }
}