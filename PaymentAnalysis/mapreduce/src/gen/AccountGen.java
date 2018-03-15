/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generates sample input for the PaymentAnalysis job.
 * <ul>
 * <li>Account - an AccountNumber, random OpenDate, and existing CustomerNumber
 * <li>Customer - a CustomerNumber,
 * <li>
 * <li>
 * </ul>
 * 
 * <p>
 * To run the program: <b>bin/hadoop jar hadoop-*-examples.jar teragen
 * 10000000000 in-dir</b>
 */
@SuppressWarnings("deprecation")
public class AccountGen extends Configured implements Tool
{
    /**
     * An input format that assigns ranges of longs to each mapper.
     */
    static class RangeInputFormat implements
			InputFormat<LongWritable, LongWritable>
    {
	/**
	 * An input split consisting of a range of numbers.
	 */
	static class RangeInputSplit implements InputSplit
	{
	    long firstAccount; /* First ssn this split will generate */
	    long accountCount; /* Number of ssns this split will generate */

	    public RangeInputSplit()
	    {
	    }

	    public RangeInputSplit(long offset, long length)
	    {
		firstAccount = offset;
		accountCount = length;
	    }

	    public long getLength() throws IOException
	    {
		return accountCount;
	    }

	    public String[] getLocations() throws IOException
	    {
		return new String[] {};
	    }

	    public void readFields(DataInput in) throws IOException
	    {
		firstAccount = WritableUtils.readVLong(in);
		accountCount = WritableUtils.readVLong(in);
	    }

	    public void write(DataOutput out) throws IOException
	    {
		WritableUtils.writeVLong(out, firstAccount);
		WritableUtils.writeVLong(out, accountCount);
	    }
	}

	/**
	 * A record reader that will generate a range of numbers.
	 */
	static class RangeRecordReader implements
				RecordReader<LongWritable, LongWritable>
	{

	    long startAccount;
	    long finishedAccounts;
	    long totalAccounts;

	    public RangeRecordReader(RangeInputSplit split)
	    {
		startAccount = split.firstAccount;
		finishedAccounts = 0;
		totalAccounts = split.accountCount;
	    }

	    public void close() throws IOException
	    {
		// NOTHING
	    }

	    public LongWritable createKey()
	    {
		return new LongWritable();
	    }

	    public LongWritable createValue()
	    {
		return new LongWritable();
	    }

	    public long getPos() throws IOException
	    {
		return finishedAccounts;
	    }

	    public float getProgress() throws IOException
	    {
		return finishedAccounts / (float) totalAccounts;
	    }

	    public boolean next(LongWritable key, LongWritable value)
	    {
		if (finishedAccounts < totalAccounts)
		{
		    key.set(startAccount + finishedAccounts);
		    value.set(startAccount);
		    finishedAccounts += 1;
		    return true;
		}
		else
		{
		    return false;
		}
	    }

	}

	public RecordReader<LongWritable, LongWritable> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter)
				throws IOException
	{
	    return new RangeRecordReader((RangeInputSplit) split);
	}

	/**
	 * Create the desired number of splits, dividing the number of rows
	 * between the mappers.
	 */
	public InputSplit[] getSplits(JobConf job, int numSplits)
	{

	    long totalAccounts = getNumberOfAccounts(job);
	    long accountsPerSplit = totalAccounts / numSplits;

	    System.out.println("Generating " + totalAccounts + " using "
					+ numSplits + " maps with step of "
		    + accountsPerSplit);

	    /* keep track of the splits we are using for each mapper */
	    InputSplit[] splits = new InputSplit[numSplits];

	    /* send a portion to each mapper */
	    long currentAccount = 0;
	    for (int split = 0; split < numSplits - 1; ++split)
	    {
		splits[split] = new RangeInputSplit(currentAccount,
						accountsPerSplit);
		currentAccount += accountsPerSplit;
	    }
	    splits[numSplits - 1] = new RangeInputSplit(currentAccount,
					totalAccounts - currentAccount);
	    return splits;
	}
    }

    static void setInitialAccount(JobConf job, long initial)
    {
	job.setLong("initial-account", initial);
    }

    static long getInitialAccount(JobConf job)
    {
	return job.getLong("initial-account", 10000000L);
    }

    static long getNumberOfAccounts(JobConf job)
    {
	return job.getLong("num-accounts", 0);
    }

    static void setNumberOfAccounts(JobConf job, long numAccounts)
    {
	job.setLong("num-accounts", numAccounts);
    }

    static void setAccountsPerCustomer(JobConf job, int numAccounts)
    {
	job.setInt("accounts-per-customer", numAccounts);
    }

    static int getAccountsPerCustomer(JobConf job)
    {
	return job.getInt("accounts-per-customer", 1);
    }

    static void setCustomersPerSsn(JobConf job, int numCustomers)
    {
	job.setInt("customers-per-ssn", numCustomers);
    }

    static int getCustomersPerSsn(JobConf job)
    {
	return job.getInt("customers-per-ssn", 1);
    }

    static void setHistogramPath(JobConf job, String path)
    {
	job.setStrings("histogram-path", path);
    }

    static String getHistogramPath(JobConf job)
    {
	return job.getStrings("histogram-path")[0];
    }

    static class RandomNameGenerator
    {

	private static Random       random	= new Random();

	private static final String FIRST_NAMES[] = { "JAMES", "JOHN",
							  "ROBERT", "MICHAEL",
							  "WILLIAM", "DAVID",
							  "RICHARD", "CHARLES",
							  "JOSEPH", "THOMAS",
							  "CHRISTOPHER",
							  "DANIEL", "PAUL",
							  "MARK",
							  "DONALD", "GEORGE",
							  "KENNETH", "STEVEN",
							  "EDWARD", "BRIAN",
							  "RONALD", "ANTHONY",
							  "KEVIN", "JASON",
							  "MATTHEW", "GARY",
							  "TIMOTHY", "JOSE",
							  "LARRY", "JEFFREY",
							  "FRANK", "SCOTT",
							  "ERIC", "STEPHEN",
							  "ANDREW", "RAYMOND",
							  "GREGORY", "JOSHUA",
							  "JERRY", "DENNIS",
							  "WALTER", "PATRICK",
							  "PETER", "HAROLD",
							  "DOUGLAS", "HENRY",
							  "CARL", "ARTHUR",
							  "RYAN", "ROGER",
							  "JOE",
							  "JUAN", "JACK",
							  "ALBERT", "JONATHAN",
							  "JUSTIN", "TERRY",
							  "GERALD", "KEITH",
							  "SAMUEL", "WILLIE",
							  "RALPH", "LAWRENCE",
							  "NICHOLAS", "ROY",
							  "BENJAMIN", "BRUCE",
							  "BRANDON", "ADAM",
							  "HARRY", "FRED",
							  "WAYNE", "BILLY",
							  "STEVE", "LOUIS",
							  "JEREMY",
							  "AARON", "RANDY",
							  "HOWARD", "EUGENE",
							  "CARLOS", "RUSSELL",
							  "BOBBY", "VICTOR",
							  "MARTIN", "ERNEST",
							  "PHILLIP", "TODD",
							  "JESSE", "CRAIG",
							  "ALAN", "SHAWN",
							  "CLARENCE", "SEAN",
							  "PHILIP", "CHRIS",
							  "JOHNNY", "EARL",
							  "JIMMY", "ANTONIO",
							  "DANNY", "BRYAN",
							  "TONY", "LUIS",
							  "MIKE", "STANLEY",
							  "LEONARD",
							  "NATHAN", "DALE",
							  "MANUEL", "MARY",
							  "PATRICIA", "LINDA",
							  "BARBARA",
							  "ELIZABETH",
							  "JENNIFER", "MARIA",
							  "SUSAN",
							  "MARGARET",
							  "DOROTHY", "LISA",
							  "NANCY", "KAREN",
							  "BETTY",
							  "HELEN", "SANDRA",
							  "DONNA", "CAROL",
							  "RUTH", "SHARON",
							  "MICHELLE", "LAURA",
							  "SARAH", "KIMBERLY",
							  "DEBORAH", "JESSICA",
							  "SHIRLEY", "CYNTHIA",
							  "ANGELA", "MELISSA",
							  "BRENDA", "AMY",
							  "ANNA", "REBECCA",
							  "VIRGINIA",
							  "KATHLEEN", "PAMELA",
							  "MARTHA",
							  "DEBRA", "AMANDA",
							  "STEPHANIE",
							  "CAROLYN",
							  "CHRISTINE",
							  "MARIE", "JANET",
							  "CATHERINE",
							  "FRANCES", "ANN",
							  "JOYCE",
							  "DIANE", "ALICE",
							  "JULIE", "HEATHER",
							  "TERESA", "DORIS",
							  "GLORIA", "EVELYN",
							  "JEAN", "CHERYL",
							  "MILDRED",
							  "KATHERINE",
							  "JOAN", "ASHLEY",
							  "JUDITH", "ROSE",
							  "JANICE", "KELLY",
							  "NICOLE", "JUDY",
							  "CHRISTINA", "KATHY" };

	private static final String LAST_NAMES[]  = { "SMITH", "JOHNSON",
							  "WILLIAMS", "JONES",
							  "BROWN", "DAVIS",
							  "MILLER", "WILSON",
							  "MOORE", "TAYLOR",
							  "ANDERSON", "THOMAS",
							  "JACKSON", "WHITE",
							  "HARRIS", "MARTIN",
							  "THOMPSON", "GARCIA",
							  "MARTINEZ",
							  "ROBINSON", "CLARK",
							  "RODRIGUEZ", "LEWIS",
							  "LEE", "WALKER",
							  "HALL", "ALLEN",
							  "YOUNG", "HERNANDEZ",
							  "KING", "WRIGHT",
							  "LOPEZ", "HILL",
							  "SCOTT", "GREEN",
							  "ADAMS", "BAKER",
							  "GONZALEZ", "NELSON",
							  "CARTER", "MITCHELL",
							  "PEREZ", "ROBERTS",
							  "TURNER", "PHILLIPS",
							  "CAMPBELL", "PARKER",
							  "EVANS", "EDWARDS",
							  "COLLINS", "STEWART",
							  "SANCHEZ", "MORRIS",
							  "ROGERS", "REED",
							  "COOK", "MORGAN",
							  "BELL", "MURPHY",
							  "BAILEY", "RIVERA",
							  "COOPER",
							  "RICHARDSON", "COX",
							  "HOWARD", "WARD",
							  "TORRES",
							  "PETERSON", "GRAY",
							  "RAMIREZ", "JAMES",
							  "WATSON", "BROOKS",
							  "KELLY", "SANDERS",
							  "PRICE", "BENNETT",
							  "WOOD", "BARNES",
							  "ROSS", "HENDERSON",
							  "COLEMAN", "JENKINS",
							  "PERRY", "POWELL",
							  "LONG", "PATTERSON",
							  "HUGHES", "FLORES",
							  "WASHINGTON",
							  "BUTLER", "SIMMONS",
							  "FOSTER", "GONZALES",
							  "BRYANT",
							  "ALEXANDER",
							  "RUSSELL", "GRIFFIN",
							  "DIAZ", "HAYES",
							  "MYERS",
							  "FORD", "HAMILTON",
							  "GRAHAM", "SULLIVAN",
							  "WALLACE", "WOODS",
							  "COLE", "WEST",
							  "JORDAN", "OWENS",
							  "REYNOLDS", "FISHER",
							  "ELLIS", "HARRISON",
							  "GIBSON", "MCDONALD",
							  "CRUZ", "MARSHALL",
							  "ORTIZ", "GOMEZ",
							  "MURRAY", "FREEMAN",
							  "WELLS", "WEBB",
							  "SIMPSON", "STEVENS",
							  "TUCKER", "PORTER",
							  "HUNTER", "HICKS",
							  "CRAWFORD", "HENRY",
							  "BOYD", "MASON",
							  "MORALES", "KENNEDY",
							  "WARREN", "DIXON",
							  "RAMOS", "REYES",
							  "BURNS", "GORDON",
							  "SHAW",
							  "HOLMES", "RICE",
							  "ROBERTSON", "HUNT",
							  "BLACK", "DANIELS",
							  "PALMER", "MILLS",
							  "NICHOLS", "GRANT",
							  "KNIGHT", "FERGUSON",
							  "ROSE", "STONE",
							  "HAWKINS", "DUNN",
							  "PERKINS", "HUDSON",
							  "SPENCER", "GARDNER",
							  "STEPHENS", "PAYNE",
							  "PIERCE", "BERRY",
							  "MATTHEWS", "ARNOLD",
							  "WAGNER", "WILLIS",
							  "RAY", "WATKINS",
							  "OLSON" };

	public static String firstName()
	{
	    return FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
	}

	public static String lastName()
	{
	    return LAST_NAMES[random.nextInt(LAST_NAMES.length)];
	}
    }

    /**
     * Categorical distribution
     * http://en.wikipedia.org/wiki/Categorical_distribution
     * 
     * f(x[i];p) = p[i]
     * 
     * p[i] is probability of seeing element i and sum(i=1 to n)(p[i] = 1)
     * 
     * f(1;p) = .33 f(2;p) = .34 ...
     * 
     * @author mholling
     * 
     */
    static class KeyGenerator
    {
	private static int     ssn_count      = 0;
	private static int     customer_count = 0;

	private static long    ssn;
	private static long    customer;

	private static boolean new_customer   = true;

	private HistogramSet   histograms;

	KeyGenerator(long initial_ssn, long initial_customer, HistogramSet set)
	{
	    ssn = initial_ssn;
	    customer = initial_customer;
	    histograms = set;
	}

	long nextSsn()
	{
	    if (ssn_count == 0)
	    {
		ssn_count = 1;
		ssn++;
	    }
	    ssn_count--;
	    return ssn;
	}

	long getSsn()
	{
	    return ssn;
	}

	long nextCustomer()
	{
	    if (customer_count == 0)
	    {
		customer_count = histograms
						.randomInt(HistogramType.CUSTOMERS_PER_ACCOUNT);
		customer++;
		new_customer = true;
	    }
	    else
	    {
		new_customer = false;
	    }
	    customer_count--;
	    return customer;
	}

	long getCustomer()
	{
	    return customer;
	}

	boolean newCustomer()
	{
	    return new_customer;
	}
    }

    static class HistogramSet
    {

	private static HashMap<HistogramType, Histogram> histograms;
	private Random				   random = new Random();
	private int[]				    selected;

	HistogramSet(JobConf job) throws IOException, InstantiationException,
				IllegalAccessException
	{

	    histograms = new HashMap<HistogramType, Histogram>();

	    FileSystem fs = FileSystem.get(job);
	    Path input = new Path(getHistogramPath(job));
	    if (!fs.exists(input))
	    {
		System.err
						.println("Please generate a valid histogram file using HistogramGen.");
		System.exit(1);
	    }
	    System.out.println("Reading histogram file: "
					+ getHistogramPath(job));

	    SequenceFile.Reader reader = new SequenceFile.Reader(fs, input, job);
	    Text key = new Text();
	    Histogram value = new Histogram();

	    while (reader.next(key, value))
	    {
		// System.out.println(value + "\n");
		addHistogram(value);
	    }

	    reader.close();
	}

	private static void addHistogram(Histogram histogram)
	{
	    histograms.put(histogram.getKey(), new Histogram(histogram));
	}

	private int randomInt(HistogramType type)
	{
	    int range = 0;
	    int index;
	    try
	    {
		selected = histograms.get(type).getValues();
		range = selected.length;
		index = random.nextInt(range);
		return selected[index];
	    } catch (IllegalArgumentException e)
	    {
		System.out.println("selected.length: " + range);
		return 0;
	    }
	}

	public String toString()
	{
	    StringBuilder builder = new StringBuilder();

	    for (Entry<HistogramType, Histogram> entry : histograms.entrySet())
	    {
		builder.append(entry.getKey());
		for (int value : entry.getValue().getValues())
		    builder.append(value + ",");
		builder.append("\n");
	    }

	    return builder.toString();
	}
    }

    /**
     * The Mapper class that given a row number, will generate the appropriate
     * output line.
     */
    public static class SortGenMapper extends MapReduceBase implements
			Mapper<LongWritable, LongWritable, Text, Text>
    {

	/* To write to different files */
	MultipleOutputs	   mos;
	private DateFormat	df		= new SimpleDateFormat(
							    "yyyy-MM-dd");
	private NumberFormat      nf		= new DecimalFormat("#.00");

	private Random	    random	    = new Random();
	private KeyGenerator      keygen;

	private HistogramSet      histograms;
	private static final int  MAX_BALANCE       = 9999;
	private static final long DAY	       = 86400000;
	private static final long MIN_DATE	  = new GregorianCalendar(
							    2008, 1, 1)
							    .getTimeInMillis();
	private static final long MAX_DATE	  = new GregorianCalendar(
							    2010, 1, 1)
							    .getTimeInMillis();

	private static int	badDebtPercentage = 80;
	private long	      numberOfAccounts;

	private long	      baseAccount;
	private long	      baseSsn;
	private long	      baseCustomer;

	private Text	      key	       = new Text();
	private Text	      value	     = new Text();

	private Date	      openDate;
	private Date	      goodStandingDate;
	private Date	      badDebtDate;
	private double	    balance;

	private byte[]	    bytes;
	private byte[]	    commaBytes	= ",".getBytes();

	/**
	 * Set the value of the output key.
	 * 
	 * @param theKey
	 */
	private void setKey(long theKey)
	{
	    bytes = Long.toString(theKey).getBytes();
	    key.clear();
	    key.append(bytes, 0, bytes.length);
	}

	/**
	 * Set a random open date for the account and append the date string to
	 * the output value.
	 * 
	 * Open Date will be random date between Jan. 01, 2008 and Jan. 01, 2010
	 */
	private void setOpenDate()
	{
	    openDate = dateInRange(MIN_DATE, MAX_DATE);
	    appendToValue(df.format(openDate));
	}

	/**
	 * Set the customer number for the account and append the string to the
	 * output value.
	 */
	private void setCustomerNumber()
	{
	    appendToValue(Long.toString(keygen.nextCustomer()));
	}

	/**
	 * Set the ssn for the account and append the string to the output
	 * value.
	 */
	private void setSsn()
	{
	    appendToValue(Long.toString(keygen.nextSsn()));
	}

	/**
	 * Set a random zip code for the account and append the string to the
	 * output value. Zip will be between 700 and 900.
	 */
	private void setZipCode3()
	{
	    appendToValue(Integer.toString(random.nextInt(200) + 700));
	}

	/**
	 * Set a random first and last name for the account and append the
	 * string to the output value.
	 */
	private void setCustomerName()
	{
	    String name = RandomNameGenerator.firstName() + ","
					+ RandomNameGenerator.lastName();
	    appendToValue(name);
	}

	/**
	 * Set a random Good Standing strategy start date for the account and
	 * append the date string to the output value.
	 * 
	 * Good Standing date will be random date between 10 and 45 days after
	 * the Open Date of the account.
	 * 
	 * @throws IOException
	 * 
	 */
	private void setGoodStandingDate(OutputCollector<Text, Text> output)
				throws IOException
	{
	    long start_date = openDate.getTime() + (DAY * 10);
	    long end_date = openDate.getTime() + (DAY * 45);

	    goodStandingDate = dateInRange(start_date, end_date);
	    value.set(",Good Standing," + df.format(goodStandingDate));
	    output.collect(key, value);
	}

	/**
	 * Set a random Bad Debt strategy start date for the account.
	 * 
	 * Bad Debt date will be random date between 90 and 100 days after the
	 * Good Standing date of the account.
	 * 
	 * @throws IOException
	 */
	private void setBadDebtDate(OutputCollector<Text, Text> output)
				throws IOException
	{
	    long start_date = goodStandingDate.getTime() + (DAY * 90);
	    long end_date = goodStandingDate.getTime() + (DAY * 100);

	    badDebtDate = dateInRange(start_date, end_date);

	    if (random.nextInt(100) < badDebtPercentage)
	    {
		value.set(",Bad Debt," + df.format(badDebtDate));
		output.collect(key, value);
	    }
	}

	/**
	 * Appends the given string to the output value.
	 * 
	 * @param string
	 */
	private void appendToValue(String string)
	{
	    bytes = string.getBytes();
	    value.append(bytes, 0, bytes.length);
	}

	/**
	 * Sets a random number of charge transactions for the account and
	 * appends the transaction to the output value.
	 * 
	 * @throws IOException
	 */
	private void setCharges(OutputCollector<Text, Text> output)
				throws IOException
	{
	    /* determine number of charges to generate */
	    int good_charges = histograms.randomInt(HistogramType.GOOD_CHARGE);
	    int bad_charges = histograms.randomInt(HistogramType.BAD_CHARGE);

	    if ((good_charges + bad_charges) > 0)
	    {
		int max_charge = (MAX_BALANCE * 100)
						/ (good_charges + bad_charges);

		/* date range for transactions */
		long start_date = openDate.getTime() + DAY;
		long end_date = badDebtDate.getTime();
		Date date;
		double amount;

		for (int i = 0; i < good_charges; i++)
		{
		    /* choose a date and charge amount */
		    date = dateInRange(start_date, end_date);
		    amount = random.nextInt(max_charge) / 100.0;

		    /* add to account balance */
		    balance += amount;

		    /* append the charge transaction */
		    value.set("," + df.format(date) + "," + nf.format(amount)
							+ "," + "Charge");
		    output.collect(key, value);
		}

		start_date = badDebtDate.getTime();
		end_date = start_date + (DAY * 90);

		for (int i = 0; i < bad_charges; i++)
		{
		    /* choose a date and charge amount */
		    date = dateInRange(start_date, end_date);
		    amount = random.nextInt(max_charge) / 100.0;

		    /* add to account balance */
		    balance += amount;

		    /* append the charge transaction */
		    value.set("," + df.format(date) + "," + nf.format(amount)
							+ "," + "Charge");
		    output.collect(key, value);
		}
	    }
	}

	/**
	 * Sets a random number of adjustment transactions for the account and
	 * appends the transaction to the output value.
	 * 
	 * @throws IOException
	 */
	private void setAdjustments(OutputCollector<Text, Text> output)
				throws IOException
	{
	    /* determine number of adjustments to generate */
	    int good_adjustments = histograms
					.randomInt(HistogramType.GOOD_ADJUSTMENT);
	    int bad_adjustments = histograms
					.randomInt(HistogramType.BAD_ADJUSTMENT);

	    if ((good_adjustments + bad_adjustments) > 0)
	    {
		int max_adjustment = (int) (balance * 100)
						/ (good_adjustments + bad_adjustments);

		Date date;
		double amount;

		/* date range for transactions */
		long start_date = openDate.getTime();
		long end_date = badDebtDate.getTime();

		for (int i = 0; i < good_adjustments; i++)
		{
		    /* choose a date and adjustment amount */
		    date = dateInRange(start_date, end_date);
		    amount = random.nextInt(max_adjustment) / 100.0;

		    /* subtract from account balance */
		    balance -= amount;

		    /* append the adjustment transaction */
		    value.set("," + df.format(date) + "," + nf.format(-amount)
							+ "," + "Adjustment");
		    output.collect(key, value);
		}

		start_date = badDebtDate.getTime();
		end_date = start_date + (DAY * 90);

		for (int i = 0; i < bad_adjustments; i++)
		{
		    /* choose a date and adjustment amount */
		    date = dateInRange(start_date, end_date);
		    amount = random.nextInt(max_adjustment) / 100.0;

		    /* subtract from account balance */
		    balance -= amount;

		    /* append the adjustment transaction */
		    value.set("," + df.format(date) + "," + nf.format(-amount)
							+ "," + "Adjustment");
		    output.collect(key, value);
		}
	    }
	}

	/**
	 * Sets a random number of payment transactions for the account and
	 * appends the transaction to the output value.
	 */
	private void setPayments(OutputCollector<Text, Text> output)
				throws IOException
	{

	    int good_payments = histograms
					.randomInt(HistogramType.GOOD_PAYMENT);
	    int bad_payments = histograms.randomInt(HistogramType.BAD_PAYMENT);

	    // System.out.println("good_payments: " + good_payments);
	    // System.out.println("bad_payments: " + bad_payments);
	    if ((good_payments + bad_payments) > 0)
	    {

		int max_payment = (int) (balance * 100)
				/ (good_payments + bad_payments);
		try
		{

		    /* date range for transactions */
		    long start_date = goodStandingDate.getTime();
		    long end_date = badDebtDate.getTime();

		    Date date;
		    double amount;

		    for (int i = 0; i < good_payments; i++)
		    {
			date = dateInRange(start_date, end_date);
			amount = random.nextInt(max_payment) / 100.0;

			/* subtract from account balance */
			balance -= amount;

			/* append the payment transaction */
			value.set("," + df.format(date) + ","
				+ nf.format(-amount)
							+ "," + "Payment");
			output.collect(key, value);
		    }

		    start_date = badDebtDate.getTime();
		    end_date = badDebtDate.getTime() + (DAY * 90);

		    for (int i = 0; i < bad_payments; i++)
		    {
			date = dateInRange(start_date, end_date);
			amount = random.nextInt(max_payment) / 100.0;

			/* subtract from account balance */
			balance -= amount;

			/* append the payment transaction */
			value.set("," + df.format(date) + ","
				+ nf.format(-amount)
							+ "," + "Payment");
			output.collect(key, value);
		    }

		} catch (IllegalArgumentException e)
		{
		    System.out.println("max_payment: " + max_payment);
		    return;
		}
	    }
	}

	/**
	 * Returns a random Date in the given range.
	 * 
	 * @param start
	 *            The start (minimum) date value.
	 * @param end
	 *            The end (maximum) date value.
	 * @return Date between start and end.
	 */
	private Date dateInRange(long start, long end)
	{
	    long date = start + Math.abs((random.nextLong() % (end - start)));
	    return new Date(date);
	}

	/**
	 * Append a comma to the output value.
	 */
	private void addComma()
	{
	    value.append(commaBytes, 0, commaBytes.length);
	}

	private void generateAccountTuple(long accountNumber,
				OutputCollector<Text, Text> output)
		throws IOException
	{
	    setKey(accountNumber);
	    value.clear();
	    addComma();
	    setOpenDate();
	    addComma();
	    setCustomerNumber();
	    output.collect(key, value);
	}

	private void generateCustomerTuple(OutputCollector<Text, Text> output)
				throws IOException
	{
	    setKey(keygen.getCustomer());
	    value.clear();
	    addComma();
	    setCustomerName();
	    addComma();
	    setSsn();
	    addComma();
	    setZipCode3();
	    output.collect(key, value);
	}

	@SuppressWarnings("unchecked")
	public void map(LongWritable account, LongWritable offset,
				OutputCollector<Text, Text> output,
		Reporter reporter)
				throws IOException
	{

	    /* get the next accountNumber from the map input */
	    long accountNumber = account.get() + baseAccount;

	    /* initialize key generator for customer number and ssn */
	    if (keygen == null)
	    {
		int firstSsn = (int) (baseSsn + offset.get());
		int firstCustomer = (int) (baseCustomer + offset.get());
		keygen = new KeyGenerator(firstSsn, firstCustomer, histograms);
	    }

	    /* ACCOUNT tuple */
	    generateAccountTuple(accountNumber,
					mos.getCollector("Account", reporter));

	    /* CUSTOMER tuple */
	    if (keygen.newCustomer())
	    {
		generateCustomerTuple(mos.getCollector("Customer", reporter));
	    }
	    /* reset key to AccountNumber */
	    setKey(accountNumber);

	    /* STRATEGY_HISTORY tuples */
	    setGoodStandingDate(mos.getCollector("StrategyHistory", reporter));
	    setBadDebtDate(mos.getCollector("StrategyHistory", reporter));

	    /* TRANSACTION tuples */
	    setCharges(mos.getCollector("Transaction", reporter));
	    setAdjustments(mos.getCollector("Transaction", reporter));

	    // System.out.println(histograms
	    // .getHistogram(HistogramType.GOOD_PAYMENT));

	    setPayments(mos.getCollector("Transaction", reporter));
	}

	@Override
	public void configure(JobConf job)
	{
	    try
	    {
		histograms = new HistogramSet(job);
		System.out.println("Histogram Set initialized:\n" + histograms);

	    } catch (Exception e)
	    {
		System.err.println("Failed to read histogram file.");
		System.exit(1);
	    }

	    mos = new MultipleOutputs(job);
	    numberOfAccounts = getNumberOfAccounts(job);
	    baseAccount = getInitialAccount(job);
	    baseSsn = baseAccount + numberOfAccounts;
	    baseCustomer = baseSsn + numberOfAccounts;
	}

	@Override
	public void close() throws IOException
	{
	    mos.close();
	}
    }

    /**
     * @param args
     *            the cli arguments
     */
    public int run(String[] args) throws IOException
    {
	JobConf job = (JobConf) getConf();

	if (args.length != 3)
	{
	    printUsage();
	}

	System.out.println("Number of map tasks: " + job.getNumMapTasks());

	setNumberOfAccounts(job, Long.parseLong(args[0]));

	setHistogramPath(job, args[1]);
	Path outFile = new Path(args[2]);

	FileSystem fs = FileSystem.get(getConf());

	if (fs.exists(outFile))
	{
	    fs.delete(outFile, true);
	}

	FileOutputFormat.setOutputPath(job, outFile);
	job.setJobName("GenerateSampleData");
	job.setJarByClass(AccountGen.class);
	job.setMapperClass(SortGenMapper.class);
	job.setNumReduceTasks(0);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setInputFormat(RangeInputFormat.class);
	job.setOutputFormat(AccountGenOutputFormat.class);

	MultipleOutputs.addNamedOutput(job, "Customer",
		AccountGenOutputFormat.class,
				Text.class, Text.class);
	MultipleOutputs.addNamedOutput(job, "Account",
		AccountGenOutputFormat.class,
				Text.class, Text.class);
	MultipleOutputs.addNamedOutput(job, "StrategyHistory",
				AccountGenOutputFormat.class, Text.class,
		Text.class);
	MultipleOutputs.addNamedOutput(job, "Transaction",
				AccountGenOutputFormat.class, Text.class,
		Text.class);

	JobClient.runJob(job);
	return 0;
    }

    public static void main(String[] args) throws Exception
    {
	int res = ToolRunner.run(new JobConf(), new AccountGen(), args);
	System.exit(res);
    }

    public static void printUsage()
    {
	System.err
				.println("Usage: AccountGen <num-accounts> <histogram-input-path> <output-path>");
	System.exit(1);
    }

}
