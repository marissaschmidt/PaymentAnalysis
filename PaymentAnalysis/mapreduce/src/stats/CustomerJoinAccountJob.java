package stats;

import io.AccountWritable;
import io.TextDatePair;
import io.TextPair;
import io.TransactionWritable;

import mapred.map.AccountMapperWithObjects;
import mapred.map.CustomerMapperWithObjects;
import mapred.reduce.CustomerJoinAccountReducerWithObjects;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import stats.PaymentAnalysisWithObjects.TextPartitioner;

@SuppressWarnings("deprecation")
public class CustomerJoinAccountJob extends Configured implements Tool 
{
    @Override
    public int run(String[] args) throws Exception
    {
	final Path customerInput = new Path(args[0] + Path.SEPARATOR_CHAR + "Customer*");
	final Path accountInput = new Path(args[0] + Path.SEPARATOR_CHAR + "Account*");

	final Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "CustomerJoinAccount");
	
	/* Define the job */
	JobConf job = new JobConf(CustomerJoinAccountJob.class);
	job.setJobName("CustomerJoinAccountJob");

	/* Map Configuration */
	MultipleInputs.addInputPath(job, customerInput, TextInputFormat.class, CustomerMapperWithObjects.class);
	MultipleInputs.addInputPath(job, accountInput, TextInputFormat.class, AccountMapperWithObjects.class);
	job.setMapOutputKeyClass(TextPair.class);
	job.setMapOutputValueClass(AccountWritable.class);
	
	/* Reduce Configuration */
	job.setReducerClass(CustomerJoinAccountReducerWithObjects.class);
	MultipleOutputs.addNamedOutput(job, "AccountWritable", SequenceFileOutputFormat.class, Text.class, AccountWritable.class);
	MultipleOutputs.addNamedOutput(job, "TransactionWritable", SequenceFileOutputFormat.class, TextDatePair.class, TransactionWritable.class);

	/* Partition and Compare Configuration */
	job.setPartitionerClass(TextPartitioner.class);
	job.setOutputKeyComparatorClass(TextPair.KeyComparator.class);
	job.setOutputValueGroupingComparator(TextPair.GroupComparator.class);

	/* I/O Configuration */
	// conf.setOutputFormat(TextOutputFormat.class);
	// TextOutputFormat.setOutputPath(conf, outputStage1);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, output);

	try 
	{
	    JobClient.runJob(job);
	} 
	catch (Exception e) 
	{
	    e.printStackTrace();
	    return 1;
	}
	return 0;
    }
}
