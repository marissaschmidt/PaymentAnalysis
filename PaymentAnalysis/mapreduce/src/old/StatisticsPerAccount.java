package old;

import io.AccountWritable;
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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This <code>StatisticsPerAccount</code> job aggregates the transaction totals
 * per account.
 * 
 * Input: <code>Transaction</code> and <code>StrategyHistory</code> records.
 * 
 * Output: An <code>Account</code> object per Account. Each Account object
 * stores the charge, adjustment and payment statistics for the Account.
 * 
 * @author Marissa Hollingsworth
 * 
 */
@SuppressWarnings("deprecation")
public class StatisticsPerAccount extends Configured implements Tool {

	static enum Counters {
		INVALID_TRANSACTION, INVALID_STRATEGY
	}

	/**
	 * Maps transaction entries by AccountNumber key.
	 */
	public static class TransactionMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, ObjectWritable> {

		public static final int COL_ACCOUNT_ID = 0;
		public static final int COL_DATE = 1;
		public static final int COL_AMOUNT = 2;
		public static final int COL_TYPE = 3;

		public void map(Object offset, Text input,
				OutputCollector<Text, ObjectWritable> output, Reporter reporter)
				throws IOException {

			String[] parts = input.toString().split(",");

			if (parts.length > 4) {
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
				transaction = new ObjectWritable(new Transaction(date, type,
						amount));
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
			Mapper<Object, Text, Text, ObjectWritable> {

		public static final int COL_ACCOUNT_ID = 0;
		public static final int COL_NAME = 1;
		public static final int COL_DATE = 2;

		public void map(Object offset, Text input,
				OutputCollector<Text, ObjectWritable> output, Reporter reporter)
				throws IOException {

			String[] parts = (input.toString()).split(",");

			String account = parts[COL_ACCOUNT_ID].trim();
			String name = parts[COL_NAME].trim();
			String startDate = parts[COL_DATE].trim();

			/* Output strategy history by accountNumber */
			ObjectWritable strategy;
			try {
				strategy = new ObjectWritable(new StrategyHistory(startDate,
						name));
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
			implements Reducer<Text, ObjectWritable, Text, AccountWritable> {

		public static final int GOOD = 0;
		public static final int BAD = 1;

		public static final int COL_DATE = 0;
		public static final int COL_TYPE = 1;
		public static final int COL_AMOUNT = 2;

		public DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
		public NumberFormat nf = DecimalFormat.getCurrencyInstance();

		public void reduce(Text key, Iterator<ObjectWritable> values,
				OutputCollector<Text, AccountWritable> output, Reporter reporter)
				throws IOException {

			/* Keep list of payments */
			List<EventWritable> events = Collections
					.synchronizedList(new ArrayList<EventWritable>());

			/* Create a new account with no account number or openDate */
			AccountWritable account = new AccountWritable();

			/* Keep track of strategy dates */
			Calendar goodStrategy = new GregorianCalendar();
			Calendar badDebt = new GregorianCalendar();

			String type;
			while (values.hasNext()) {
				EventWritable next = (EventWritable) values.next().get();
				type = next.getType().toString();

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
			int period = 0;
			double[] adjustments = new double[2];
			double[] charges = new double[2];
			double[][] payments = new double[2][4];

			/* Sort by date and find sums */
			Collections.sort(events);

			synchronized (events) {
				ListIterator<EventWritable> itr = events.listIterator();
				while (itr.hasNext()) {

					EventWritable next = itr.next();
					type = next.getType().toString();

					if (type.equals(StrategyHistory.GOOD_STANDING))
						strategy = GOOD;
					else if (type.equals(StrategyHistory.BAD_DEBT)) {
						period = 0;
						strategy = BAD;
					} else if (type.equals(Transaction.ADJUSTMENT)) {
						adjustments[strategy] += ((Transaction) next)
								.getAmount();
					} else if (type.equals(Transaction.CHARGE)) {
						charges[strategy] += ((Transaction) next).getAmount();
					} else if (type.equals(Transaction.PAYMENT)) {
						payments[strategy][period] += ((Transaction) next)
								.getAmount();
					} else {
						period = Integer.parseInt(type) / 30;
						payments[strategy][period] = payments[strategy][period - 1];
					}

				}
			}

			account.setBadDebtAdjustments(adjustments[BAD]);
			account.setGoodStandingAdjustments(adjustments[GOOD]);

			account.setBadDebtCharges(charges[BAD]);
			account.setGoodStandingCharges(charges[GOOD]);

			account.setGoodStandingPayments30Day(payments[GOOD][0]);
			account.setGoodStandingPayments60Day(payments[GOOD][1]);
			account.setGoodStandingPayments90Day(payments[GOOD][2]);
			account.setGoodStandingPaymentsOther(payments[GOOD][3]);

			account.setBadDebtPayments30Day(payments[BAD][0]);
			account.setBadDebtPayments60Day(payments[BAD][1]);
			account.setBadDebtPayments90Day(payments[BAD][2]);
			account.setBadDebtPaymentsOther(payments[BAD][3]);

			output.collect(key, account);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
	
		Path strategyInput = new Path(args[0] + Path.SEPARATOR_CHAR
				+ "StrategyHistory*");		
		
		Path transactionInput = new Path(args[0] + Path.SEPARATOR_CHAR
				+ "Transaction*");

		Path output = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "AccountStatistics");

		int mode = 0;
		if (args.length == 3)
			mode = Integer.parseInt(args[2]);

		if (args.length == 4 && args[3].equals("true")) {

			FileSystem fs = FileSystem.get(getConf());

			if (fs.exists(output))
				fs.delete(output, true);
		}
		JobConf conf = new JobConf(StatisticsPerAccount.class);
		conf.setJobName("AccountStatistics");		
		MultipleInputs.addInputPath(conf, strategyInput, TextInputFormat.class,
				StrategyMapper.class);
		MultipleInputs.addInputPath(conf, transactionInput,
				TextInputFormat.class, TransactionMapper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(ObjectWritable.class);
		conf.setReducerClass(StrategyTransactionReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(AccountWritable.class);

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
		System.out
				.println("Usage: StatisticsPerAccount <input path> <output path> [output mode(0 for sequence | 1 for text)] [Remove existing output (true | false)]");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
		}

		int exitCode = ToolRunner.run(new StatisticsPerAccount(), args);
		System.exit(exitCode);
	}
}
