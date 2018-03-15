package old;

import io.AccountWritable;
import io.CustomerWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This <code>AccountsPerSsn</code> job groups the accounts by ssn in order
 * to calculate the previous account values for each account. 
 * 
 * Input: <code>CustomersPerSsn</code> records.
 * 
 * Output: A new <code>Account</code> object per SSN. Each Account object 
 * stores the charge, adjustment and payment statistics for current Account
 * and for the past accounts.
 * 
 * @author Marissa Hollingsworth
 * 
 */
@SuppressWarnings("deprecation")
public class AccountsPerSsn  extends Configured implements Tool {

	/**
	 * Combines the output from the AccountMapper and AccountStatistics 
	 * into a single Account object. 
	 */
	public static class CustomerReducer extends MapReduceBase
			implements Reducer<Text, CustomerWritable, Text, AccountWritable> {

		public void reduce(Text ssn, Iterator<CustomerWritable> customers,
				OutputCollector<Text, AccountWritable> output, Reporter reporter)
				throws IOException {

			/* past accounts for this ssn */
			ArrayList<AccountWritable> accounts = new ArrayList<AccountWritable>();
			
			/* iterate the list of customers with matching ssn and
			 * add the accounts for the customer to the list of 
			 * accounts for the ssn */
			while (customers.hasNext()) {
				CustomerWritable next = customers.next();
				Collections.addAll(accounts, next.getAccounts());
			}

			Collections.sort(accounts);
			AccountWritable[] accountList = accounts.toArray(new AccountWritable[0]);
			
			int prevAcctCount = 0;
			double prevAcctGoodCharges = 0;
			double prevAcctGoodAdjustments = 0;
			double prevAcctGoodPayments = 0;
			double prevAcctBadPayments = 0;
			
			/* go through the list of accounts and sum the past 
			 * account values for each account. */
			for(int i = 0; i < accountList.length; i++) {
				AccountWritable account = new AccountWritable(accountList[i]);
				
				account.setPreviousAccountCount(prevAcctCount);
				account.setPreviousAccountGoodStandingCharges(prevAcctGoodCharges);
				account.setPreviousAccountGoodStandingAdjustments(prevAcctGoodAdjustments);
				account.setPreviousAccountGoodStandingPayments(prevAcctGoodPayments);
				account.setPreviousAccountBadDebtPayments(prevAcctBadPayments);
				
				/* output account with past account values set. */
				output.collect(account.getAccountNumber(), account);
				
				prevAcctCount++;
				prevAcctGoodCharges += accountList[i].getGoodStandingCharges();
				prevAcctGoodAdjustments += accountList[i].getGoodStandingAdjustments();
				prevAcctGoodPayments += accountList[i].getGoodStandingPayments();
				prevAcctBadPayments += accountList[i].getBadDebtPayments();
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		Path input = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "CustomersPerSsn");

		Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "Results");
		
		JobConf conf = new JobConf(AccountsPerSsn.class);
		conf.setJobName("Results");

		conf.setInputFormat(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(conf, input);
		conf.setMapperClass(IdentityMapper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(CustomerWritable.class);
		
		conf.setReducerClass(CustomerReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(AccountWritable.class);
		
		//conf.setOutputFormat(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setOutputPath(conf, output);
		conf.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(conf, output);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	public static void printUsage() {
		System.out.println("Usage: AccountsPerSsn <input path> <output path>");
		System.exit(0);
	}
	
	public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                   printUsage();
            }
		
		int	exitCode = ToolRunner.run(new AccountsPerSsn(), args);
			System.exit(exitCode);
	}
	
}

