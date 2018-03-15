package gen;

import io.InvalidDateException;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import old.EventWritable;
import old.StrategyHistory;
import old.Transaction;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This <code>TransactionStatistics</code> job counts the number of 
 * each transaction type per account. This job is used as input to the
 * {@link HistogramGen} job.
 * 
 * Input: <code>Transaction</code> and <code>StrategyHistory</code> records.
 * 
 * Output: The type and total number of each transaction type. 
 * 
 * @author Marissa Hollingsworth
 * 
 */
@SuppressWarnings("deprecation")
public class TransactionStatistics extends Configured implements Tool 
{
    static enum Counters 
    {
	INVALID_TRANSACTION, INVALID_STRATEGY, TRANSACTIONS
    }

    /**
     * Maps transaction entries by AccountNumber key.
     */
    public static class TransactionMapper extends MapReduceBase implements 
    	Mapper<Object, Text, Text, ObjectWritable> 
    {
	public static final int COL_ACCOUNT_ID = 0;
	public static final int COL_DATE = 1;
	public static final int COL_AMOUNT = 2;
	public static final int COL_TYPE = 3;

	public void map(Object offset, Text input,
			OutputCollector<Text, ObjectWritable> output, Reporter reporter)
			throws IOException 
	{
	    String[] parts = input.toString().split(",");

	    if (parts.length > 4) 
	    {
		parts[2] = (parts[2] + parts[3]).replaceAll("[ \"]", "");
		parts[3] = parts[4];
	    }

	    String account = parts[COL_ACCOUNT_ID].trim();
	    String date = parts[COL_DATE].trim();
	    String amount = parts[COL_AMOUNT].trim();
	    String type = parts[COL_TYPE].trim();

	    /* Output transactions by accountNumber */
	    ObjectWritable transaction;
	    try {
		transaction = new ObjectWritable(new Transaction(date, type, amount));
		output.collect(new Text(account), transaction);
	    } catch (InvalidDateException e) {
		reporter.incrCounter(Counters.INVALID_TRANSACTION, 1);
		return;
		}
	}
    }

    /**
     * Maps strategy history entries to AccountNumber key.
     */
    public static class StrategyMapper extends MapReduceBase implements
    	Mapper<Object, Text, Text, ObjectWritable> 
    {
	public static final int COL_ACCOUNT_ID = 0;
	public static final int COL_NAME = 1;
	public static final int COL_DATE = 2;

	public void map(Object offset, Text input,
			OutputCollector<Text, ObjectWritable> output, Reporter reporter)
			throws IOException 
	{
	    String[] parts = (input.toString()).split(",");
	    String account = parts[COL_ACCOUNT_ID].trim();
	    String name = parts[COL_NAME].trim();
	    String startDate = parts[COL_DATE].trim();

	    /* Output strategy history by accountNumber */
	    ObjectWritable strategy;
	    try {
		strategy = new ObjectWritable(new StrategyHistory(startDate, name));
		output.collect(new Text(account), strategy);
	    } catch (InvalidDateException e) {
		reporter.incrCounter(Counters.INVALID_STRATEGY, 1);
		return;
	    }
	}
    }

    /**
     * Accepts key/value pairs from <code>TransactionMapper</code> and
     * <code>StrategyHistoryMapper</code>.
     * 
     * Iterates through the list of events and sums the values for each account.
     * The output is an <code>Account</code> object per
     * <code>AccountNumber</code>.
     */
    public static class StrategyTransactionReducer extends MapReduceBase
    	implements Reducer<Text, ObjectWritable, IntWritable, IntWritable> 
    {
	public static final int GOOD = 0;
	public static final int BAD = 1;
	public static final int CHARGE = 0;   //goodcharge = 0, badcharge = 1
	public static final int ADJUSTMENT = 2;	//goodadjust = 2, badadjust = 3
	public static final int PAYMENT = 4;	//goodpayment30 (+good+period) = 4 

	public static final int COL_DATE = 0;
	public static final int COL_TYPE = 1;
	public static final int COL_AMOUNT = 2;
	public DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	public NumberFormat nf = DecimalFormat.getCurrencyInstance();

	public void reduce(Text key, Iterator<ObjectWritable> values,
		OutputCollector<IntWritable, IntWritable> output,
		Reporter reporter) throws IOException 
	{
	    /* Keep list of payments */
	    List<EventWritable> events = new ArrayList<EventWritable>();

	    /* Keep track of strategy dates */
	    Calendar goodStrategy = new GregorianCalendar();
	    Calendar badDebt = new GregorianCalendar();

	    while (values.hasNext()) {
		EventWritable next = (EventWritable) values.next().get();
		String type = next.getType().toString();
		events.add(next);
		/* Set good standing dates */
		if (type.equals(StrategyHistory.GOOD_STANDING)) {
			goodStrategy.setTime(next.getDate().get());
			goodStrategy.add(Calendar.DAY_OF_MONTH, 30);
			events.add(new StrategyHistory(goodStrategy.getTime(), "30"));
			goodStrategy.add(Calendar.DAY_OF_MONTH, 30);
			events.add(new StrategyHistory(goodStrategy.getTime(), "60"));
			goodStrategy.add(Calendar.DAY_OF_MONTH, 30);
			events.add(new StrategyHistory(goodStrategy.getTime(), "90"));
		}
		/* Set bad debt dates */
		else if (type.equals(StrategyHistory.BAD_DEBT)) {
		    badDebt.setTime(next.getDate().get());
		    badDebt.add(Calendar.DAY_OF_MONTH, 30);
		    events.add(new StrategyHistory(badDebt.getTime(), "30"));
		    badDebt.add(Calendar.DAY_OF_MONTH, 30);
		    events.add(new StrategyHistory(badDebt.getTime(), "60"));
		    badDebt.add(Calendar.DAY_OF_MONTH, 30);
		    events.add(new StrategyHistory(badDebt.getTime(), "90"));
		}
	    }
	    int strategy = GOOD;
	    int[] transaction_count = new int[6];
	    
	    /* Sort by date and find sums */
	    Collections.sort(events);
	    for(EventWritable event : events) {
		String type = event.getType().toString();
		if (type.equals(StrategyHistory.GOOD_STANDING)) { strategy = GOOD; } 
		else if (type.equals(StrategyHistory.BAD_DEBT)) { strategy = BAD; }
		else if (type.equals(Transaction.ADJUSTMENT)) { transaction_count[ADJUSTMENT+strategy]++; }
		else if (type.equals(Transaction.CHARGE)) { transaction_count[CHARGE+strategy]++;}
		else if (type.equals(Transaction.PAYMENT)) { transaction_count[PAYMENT+strategy]++;}
	    }
	    // output one for each strategy
	    for(int i = 0; i < transaction_count.length; i++){
		output.collect(new IntWritable(i), new IntWritable(transaction_count[i]));
	    }		
	}
    }

	@Override
	public int run(String[] args) throws Exception {

		Path strategyInput = new Path(args[0] + Path.SEPARATOR_CHAR
				+ "StrategyHistory*");
		Path transactionInput = new Path(args[0] + Path.SEPARATOR_CHAR
				+ "Transaction*");

		Path output = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "TransactionStatistics");

		int mode = 0;
		if (args.length == 3) 
		{
			mode = Integer.parseInt(args[2]);
		}
		
		if (args.length == 4 && args[3].equals("true")) {

			FileSystem fs = FileSystem.get(getConf());

			if (fs.exists(output))
				fs.delete(output, true);
		}
		
		JobConf conf = new JobConf(TransactionStatistics.class);
		conf.setJobName("HistogramGen");

		MultipleInputs.addInputPath(conf, strategyInput, TextInputFormat.class,
				StrategyMapper.class);
		MultipleInputs.addInputPath(conf, transactionInput,
				TextInputFormat.class, TransactionMapper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(ObjectWritable.class);
		conf.setReducerClass(StrategyTransactionReducer.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		if (mode == 0) {
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(conf, output);
		} else {
			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, output);
		}
		
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}		
		
		return 0;
	}

	public static void printUsage() {
		System.out.println("Usage: StatisticsPerAccount <input path> <output path> [<output-mode (sequence|text)>] [Remove existing output (true | false)]");
		System.exit(1);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
		}

		int exitCode = ToolRunner.run(new TransactionStatistics(), args);
		System.exit(exitCode);
	}
}
