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
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This <code>CustomersPerSsn</code> job aggregates the Customers per
 * SSN.
 * 
 * Input: <code>AccountsPerCustomer</code> and <code>Customer</code> records.
 * 
 * Output: A <code>Customer</code> object per SSN. Each Customer object stores 
 * a list of Accounts that it owns. Each Account object stores the charge, 
 * adjustment and payment statistics for the Account.
 * 
 * @author Marissa Hollingsworth
 * 
 */
@SuppressWarnings("deprecation")
public class CustomersPerSsn extends Configured implements Tool {

	/**
	 * Maps customer information to the unique CustomerNumber key.
	 */
	public static class CustomerMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, CustomerWritable> {

		public static final int COL_CUSTOMER_NUM = 0;
		public static final int COL_FIRST_NAME = 1;
		public static final int COL_LAST_NAME = 2;
		public static final int COL_SSN = 3;
		public static final int COL_ZIP_CODE_3 = 4;

		public void map(Object offset, Text input,
				OutputCollector<Text, CustomerWritable> output, Reporter reporter)
				throws IOException {

			String[] parts = (input.toString()).split(",");

			String customerNumber = parts[COL_CUSTOMER_NUM].trim();
			String ssn = parts[COL_SSN].trim();
			String zipCode3 = parts[COL_ZIP_CODE_3].trim();

			CustomerWritable customer = new CustomerWritable(ssn, zipCode3);

			/* output customer number and customer information (ssn and zipcode3)*/
			output.collect(new Text(customerNumber), customer);
		}
	}

	/**
	 * Generates a Customer object from the Account objects output by the
	 * AccountsPerCustomer job. Maps this customer to the unique CustomerNumber
	 * key.
	 */
	public static class AccountToCustomerMapper extends MapReduceBase implements
			Mapper<Text, AccountWritable, Text, CustomerWritable> {

		public void map(Text customerNumber, AccountWritable account,
				OutputCollector<Text, CustomerWritable> output, Reporter reporter)
				throws IOException {

			/* empty list of Accounts with the CustomerNumber */
			AccountWritable[] accounts = new AccountWritable[1];
			
			/* add account with matching customer number to the list. */
			accounts[0] = new AccountWritable(account);

			/* create a customer with the past account list */
			CustomerWritable customer = new CustomerWritable(accounts);

			/* output customer number and customer with past accounts */
			output.collect(customerNumber, customer);
		}
	}

	/**
	 * Combines the output from the AccountMapper and AccountStatistics into a
	 * single Account object.
	 */
	public static class CustomerReducer extends MapReduceBase implements
			Reducer<Text, CustomerWritable, Text, CustomerWritable> {

		public void reduce(Text customerNumber, Iterator<CustomerWritable> customers,
				OutputCollector<Text, CustomerWritable> output, Reporter reporter)
				throws IOException {

			/* create new customer with the customer number */
			CustomerWritable result = new CustomerWritable(customerNumber);
			Text ssn = new Text();
			
			/* List to hold the accounts with the same customer number */
			ArrayList<AccountWritable> accounts = new ArrayList<AccountWritable>();

			/* iterate list of customers with the same customer number */
			while (customers.hasNext()) {
				CustomerWritable next = customers.next();
				
				/* set customer ssn */
				if (next.getCustomerNumber().length() != 0)
					ssn.set(next.getCustomerNumber());
				
				/* set customer zip code three */
				if (next.getZipCode3() != 0)
					result.setZipCode3(next.getZipCode3());
				
				/* add all the accounts owned by the customer to 
				 * the list of accounts */
				Collections.addAll(accounts, next.getAccounts());
			}
			
			/* set zipcodes of each account owned by this customer */
			for(AccountWritable a : accounts)
				a.setZipCode3(result.getZipCode3());
			
			/* set accounts owned by this customer */
			result.setAccounts(new AccountArrayWritable(accounts
					.toArray(new AccountWritable[0])));
			


			/* output ssn of customer and customer information/past accounts */
			output.collect(ssn, result);
		}
	}

//	public static class AccountPerCustomerReducer extends MapReduceBase
//			implements Reducer<Text, Customer, Text, Account> {
//
//		public void reduce(Text customerNumber, Iterator<Customer> customers,
//				OutputCollector<Text, Account> output, Reporter reporter)
//				throws IOException {
//
//			Customer result = new Customer(customerNumber);
//			Text ssn = new Text();
//			
//			System.out.println("Iterating customers for [" + customerNumber
//					+ "]");
//
//			while (customers.hasNext()) {
//				Customer next = customers.next();
//				System.out.println(next);
//
//				if (next.getCustomerNumber().length() != 0)
//					ssn.set(next.getCustomerNumber());
//				if (next.getZipCode3().get() != 0) {
//					result.setZipCode3(next.getZipCode3().get());
//				}
//				if (next.getAccounts().length > 0) {
//
//					// Account[]
//					// for(int i = 0; i < next.getAccounts().length ; i++)
//					// accounts.add()
//					ArrayList<Account> accounts = new ArrayList<Account>();
//					Collections.addAll(accounts, next.getAccounts());
//
//					Collections.sort(accounts);
//
//					Account[] accountList = accounts.toArray(new Account[0]);
//
//					int prevAcctCount = 0;
//					double prevAcctGoodCharges = 0;
//					double prevAcctGoodAdjustments = 0;
//					double prevAcctGoodPayments = 0;
//					double prevAcctBadPayments = 0;
//
//					for (int i = 1; i <= accountList.length; i++) {
//
//						accountList[i-1].setPreviousAccountCount(prevAcctCount);
//						accountList[i-1].setPreviousAccountGoodStandingCharges(prevAcctGoodCharges);
//						accountList[i-1].setPreviousAccountGoodStandingAdjustments(prevAcctGoodAdjustments);
//						accountList[i-1].setPreviousAccountGoodStandingPayments(prevAcctGoodPayments);
//						accountList[i-1].setPreviousAccountBadDebtPayments(prevAcctBadPayments);
//						
//						output.collect(accountList[i-1].getAccountNumber(), accountList[i-1]);
//
//						prevAcctCount++;
//						
//						prevAcctGoodCharges += accountList[i-1].getGoodStandingCharges();
//						prevAcctGoodAdjustments += accountList[i-1].getGoodStandingAdjustments();
//						prevAcctGoodPayments += accountList[i-1].getGoodStandingPayments();
//						prevAcctBadPayments += accountList[i-1].getBadDebtPayments();
//
//
//					}
//				}
//			}
//
//			// result.setAccounts(new AccountArrayWritable(accounts.toArray(new
//			// Account[0])));
//
//			System.out.println("Resulting Account: ");
//			System.out.println(result);
//			System.out.println();
//			// output.collect(customerNumber, result);
//		}
//	}

	@Override
	public int run(String[] args) throws Exception {

		Path customerInput = new Path(args[0] + Path.SEPARATOR_CHAR
				+ "Customer*");
		Path accountInput = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "AccountsPerCustomer");

		Path output = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "CustomersPerSsn");
		
		int mode = 0;
		if (args.length == 3)
			mode = Integer.parseInt(args[2]);

		JobConf conf = new JobConf(CustomersPerSsn.class);
		conf.setJobName("CustomersPerSsn");

		MultipleInputs.addInputPath(conf, customerInput, TextInputFormat.class,
				CustomerMapper.class);
		MultipleInputs.addInputPath(conf, accountInput,
				SequenceFileInputFormat.class, AccountToCustomerMapper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(CustomerWritable.class);
		conf.setReducerClass(CustomerReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(CustomerWritable.class);
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
		System.out.println("Usage: CustomersPerSsn <input path> <output path> [output mode(0 for sequence | 1 for text)]");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
		}

		int exitCode = ToolRunner.run(new CustomersPerSsn(), args);
		System.exit(exitCode);
	}

}
