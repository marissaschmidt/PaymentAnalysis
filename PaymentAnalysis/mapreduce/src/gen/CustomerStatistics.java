package gen;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This <code>CustomerStatistics</code> counts the number of Accounts
 * per Customer. This job is used as input to the {@link HistogramGen} job.
 * 
 * Input: <code>Account</code> records.
 * 
 * Output: The number of Accounts per CustomerNumber.
 * 
 * @author Marissa Hollingsworth
 * 
 */
@SuppressWarnings("deprecation")
public class CustomerStatistics extends Configured implements Tool {

    /**
     * Maps a value of one to each customer number.
     */
    public static class CustomerMapper extends MapReduceBase 
    	implements Mapper<Object, Text, Text, IntWritable> 
    {
	public static final int COL_ACCOUNT_NUM = 0;
	public static final int COL_OPEN_DATE = 1;
	public static final int COL_CUSTOMER_NUM = 2;
	public static final IntWritable ONE = new IntWritable(1);

	public void map(Object offset, Text input, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter) throws IOException 
	{
	    String[] parts = (input.toString()).split(",");
	    String customerNumber = parts[COL_CUSTOMER_NUM].trim();
	    output.collect(new Text(customerNumber), ONE);
	}
    }
    /**
     * Count the number of accounts per customer.
     */
    public static class CustomerReducer extends MapReduceBase implements 
    	Reducer<Text, IntWritable, IntWritable, IntWritable> 
    {
	public static final IntWritable histogramType 
			= new IntWritable(HistogramType.CUSTOMERS_PER_ACCOUNT.ordinal());
	
	public void reduce(Text customerNumber, Iterator<IntWritable> values,
			   OutputCollector<IntWritable, IntWritable> output,
			   Reporter reporter) throws IOException 
	{
	    int count = 0;
	    while (values.hasNext()) 
	    {
		count += values.next().get();
	    }
	    /* output the type of histogram and the count */
	    output.collect(histogramType, new IntWritable(count));
	}
    }

	@Override
	public int run(String[] args) throws Exception {

		Path input = new Path(args[0] + Path.SEPARATOR_CHAR + "Account*");
		Path output = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "CustomerStatistics");

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

		JobConf conf = new JobConf(CustomerStatistics.class);
		conf.setJobName("CustomerStatistics");

		conf.setInputFormat(TextInputFormat.class);
		TextInputFormat.setInputPaths(conf, input);
		conf.setMapperClass(CustomerMapper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setReducerClass(CustomerReducer.class);

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
		System.out
				.println("Usage: CustomerStatistics <input path> <output path> [<output-mode (sequence|text)>] [Remove existing output (true | false)]");
		System.exit(1);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
		}

		int exitCode = ToolRunner.run(new CustomerStatistics(), args);
		System.exit(exitCode);
	}

}
