package gen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generates a set of {@link Histogram} objects for a given set
 * of input data. Requires <code>CustomerStatistics.java</code> and 
 * <code>TransactionStatistics.java</code> to parse and count values 
 * from the input data.
 * 
 * Required input: 
 * 		Transaction.csv files
 * 		Customer.csv files
 * 
 * Histograms output for the following values:
 * 			Number of Good Standing Charges per Account 
 * 			Number of Bad Debt Charges per Account 
 * 			Number of Good Standing Adjustments per Account
 * 			Number of Bad Debt Adjustments per Account
 * 			Number of Good Standing Payments per Account
 * 			Number of Bad Debt Payments per Account
 * 			Number of Customers per Account
 * 
 * @author Marissa Hollingsworth
 *
 */
@SuppressWarnings("deprecation")
public class HistogramGen extends Configured implements Tool {

	public static class HistogramReducer extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, Text, Histogram> {

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<Text, Histogram> output, Reporter reporter)
				throws IOException {
			
			/* What type of histogram is this? */
			HistogramType type = HistogramType.getType(key.get());
			
			/* add all the values to a temp list */
			ArrayList<Integer> temp = new ArrayList<Integer>();								
			while (values.hasNext()) {
				temp.add(values.next().get());
			}
			
			/* sort list and create a new Histogram from data */
			Collections.sort(temp);
			Histogram histogram = new Histogram(type, temp.toArray(new Integer[0]));
			
			output.collect(new Text(type.toString()), histogram);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Path transactionInput = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "TransactionStatistics");
		Path customerInput = new Path(args[1] + Path.SEPARATOR_CHAR
				+ "CustomerStatistics");

		Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "Histograms");

		int mode = 0;
		if (args.length >= 3) 
		{
			mode = Integer.parseInt(args[2]);
		}
		if (args.length == 4 && args[3].equals("true")) {

			FileSystem fs = FileSystem.get(getConf());

			if (fs.exists(output))
				fs.delete(output, true);
		}

		JobConf conf = new JobConf(HistogramGen.class);
		conf.setJobName("HistogramGen");

		MultipleInputs.addInputPath(conf, transactionInput,
				SequenceFileInputFormat.class, IdentityMapper.class);
		MultipleInputs.addInputPath(conf, customerInput,
				SequenceFileInputFormat.class, IdentityMapper.class);

		// conf.setInputFormat(SequenceFileInputFormat.class);
		// SequenceFileInputFormat.setInputPaths(conf, input);
		// conf.setMapperClass(IdentityMapper.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setReducerClass(HistogramReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Histogram.class);

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
				.println("Usage: HistogramGen <input path> <output path> [<output-mode (sequence|text)>] [Remove existing output (true | false)]");
		System.exit(1);
	}

	public static void main(String[] args) throws Exception {
		if( args.length < 2 )
			printUsage();
		
		String mode = "sequence";
		if( args.length == 3 )
			mode = args[2];
		
		if(mode.equals("text") || mode.equals("t"))
			mode = "1";
		else if(mode.equals("sequence") || mode.equals("s"))
			mode = "0";
		else
			printUsage();
		
		/* set the exit code to sequence for first two jobs */
		int exitCode = ToolRunner.run(new TransactionStatistics(), args);
		if (exitCode == 0)
			exitCode = ToolRunner.run(new CustomerStatistics(), args);
		if (exitCode == 0) {
			/* set exit code to mode for last job */
			exitCode = ToolRunner.run(new HistogramGen(), args);
		}
		System.exit(exitCode);
	}
}
