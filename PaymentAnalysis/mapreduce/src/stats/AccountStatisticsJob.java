package stats;

import io.AccountWritable;
import io.TextDatePair;
import io.TransactionWritable;

import mapred.map.StrategyHistoryMapperWithObjects;
import mapred.map.TransactionMapperWithObjects;
import mapred.reduce.AccountStatisticsReducerWithObjects;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import stats.PaymentAnalysisWithObjects.DatePartitioner;

@SuppressWarnings("deprecation")
public class AccountStatisticsJob extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
	Path customerJoinAccountInput = new Path(args[1] + Path.SEPARATOR_CHAR + "CustomerJoinAccount" + Path.SEPARATOR_CHAR + "TransactionWritable");
	Path strategyInput = new Path(args[0] + Path.SEPARATOR_CHAR + "StrategyHistory*");
	Path transactionInput = new Path(args[0] + Path.SEPARATOR_CHAR + "Transaction*");
	
	Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "AccountStatistics");
	
	JobConf job = new JobConf(AccountStatisticsJob.class);
	job.setJobName("AccountStatisticsJob");

	/* Map Configuration */
	MultipleInputs.addInputPath(job, customerJoinAccountInput, SequenceFileInputFormat.class, IdentityMapper.class);
	MultipleInputs.addInputPath(job, strategyInput, TextInputFormat.class, StrategyHistoryMapperWithObjects.class);
	MultipleInputs.addInputPath(job, transactionInput, TextInputFormat.class, TransactionMapperWithObjects.class);

	job.setMapOutputKeyClass(TextDatePair.class);
	job.setMapOutputValueClass(TransactionWritable.class);

	/* Reduce Configuration */
	job.setReducerClass(AccountStatisticsReducerWithObjects.class);
	MultipleOutputs.addNamedOutput(job, "AccountWritable", SequenceFileOutputFormat.class, Text.class, AccountWritable.class);
	MultipleOutputs.addNamedOutput(job, "TransactionWritable", SequenceFileOutputFormat.class, TextDatePair.class, TransactionWritable.class);

	/* Partition and Compare Configuration */
	job.setPartitionerClass(DatePartitioner.class);
	job.setOutputKeyComparatorClass(TextDatePair.KeyComparator.class);
	job.setOutputValueGroupingComparator(TextDatePair.GroupComparator.class);

	/* I/O Configuration */
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
