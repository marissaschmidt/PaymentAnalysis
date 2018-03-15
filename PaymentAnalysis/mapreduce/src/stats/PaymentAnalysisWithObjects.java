package stats;

import io.TextDatePair;
import io.TextPair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
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
public class PaymentAnalysisWithObjects
{
    /**
     * Partitions the data to make sure that all of the output with the same key
     * values are sent to one reducer.
     */
    public static class TextPartitioner implements Partitioner<TextPair, Text>
    {
	@Override
	public void configure(JobConf job)
	{
	}

	@Override
	public int getPartition(TextPair key, Text value, int numPartitions)
	{
	    return Math.abs(key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
    }

    /**
     * Partitions the data to make sure that all of the output with the same key
     * values are sent to one reducer.
     */
    public static class DatePartitioner implements Partitioner<TextDatePair, Text>
    {
	@Override
	public void configure(JobConf job)
	{
	}

	@Override
	public int getPartition(TextDatePair key, Text value, int numPartitions)
	{
	    return Math.abs(key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
    }
//
//    @Override
//    public int run(String[] args) throws Exception
//    {	
//	Path customerInput = new Path(args[0] + Path.SEPARATOR_CHAR + "Customer*");
//	Path accountInput = new Path(args[0] + Path.SEPARATOR_CHAR + "Account*");
//	Path strategyInput = new Path(args[0] + Path.SEPARATOR_CHAR + "StrategyHistory*");
//	Path transactionInput = new Path(args[0] + Path.SEPARATOR_CHAR + "Transaction*");
//
//	Path outputStage1 = new Path(args[1] + Path.SEPARATOR_CHAR + "stage1");
//	Path outputStage2 = new Path(args[1] + Path.SEPARATOR_CHAR + "stage2");
//	Path outputStage3 = new Path(args[1] + Path.SEPARATOR_CHAR + "stage3");
//
//	int mode = 0;
//	if (args.length >= 3)
//	{
//	    mode = Integer.parseInt(args[2]);
//	}
//	if (args.length == 4 && args[3].equals("true"))
//	{
//	    FileSystem fs = FileSystem.get(getConf());
//	    Path output = new Path(args[1]);
//	    if (fs.exists(output))
//	    {
//		fs.delete(output, true);
//	    }
//	}
//
//	/* Define the job */
//	JobConf conf = new JobConf(PaymentAnalysisWithObjects.class);
//	conf.setJobName("JoinCustomerAccount");
//
//	/* Map Configuration */
//	MultipleInputs.addInputPath(conf, customerInput, TextInputFormat.class,
//				mapred.map.CustomerMapper.class);
//	MultipleInputs.addInputPath(conf, accountInput, TextInputFormat.class,
//				mapred.map.AccountMapper.class);
//	conf.setMapOutputKeyClass(TextPair.class);
//	conf.setMapOutputValueClass(Text.class);
//
//	/* Reduce Configuration */
//	conf.setReducerClass(mapred.reduce.CustomerJoinAccountReducer.class);
//	conf.setOutputKeyClass(TextDatePair.class);
//	conf.setOutputValueClass(Text.class);
//
//	/* Partition and Compare Configuration */
//	conf.setPartitionerClass(TextPartitioner.class);
//	conf.setOutputKeyComparatorClass(TextPair.KeyComparator.class);
//	conf.setOutputValueGroupingComparator(TextPair.GroupComparator.class);
//
//	/* I/O Configuration */
//	// conf.setOutputFormat(TextOutputFormat.class);
//	// TextOutputFormat.setOutputPath(conf, outputStage1);
//	conf.setOutputFormat(SequenceFileOutputFormat.class);
//	SequenceFileOutputFormat.setOutputPath(conf, outputStage1);
//
//	try
//	{
//	    JobClient.runJob(conf);
//	} 
//	catch (Exception e)
//	{
//	    e.printStackTrace();
//	    return 1;
//	}
//
//	conf = new JobConf(PaymentAnalysisWithObjects.class);
//	conf.setJobName("JoinAccountStrategyTransaction");
//
//	/* Map Configuration */
//	MultipleInputs.addInputPath(conf, outputStage1, SequenceFileInputFormat.class,
//		IdentityMapper.class);
//
//	MultipleInputs.addInputPath(conf, strategyInput, TextInputFormat.class,
//		mapred.map.StrategyHistoryMapper.class);
//
//	MultipleInputs.addInputPath(conf, transactionInput, TextInputFormat.class,
//		mapred.map.TransactionMapper.class);
//
//	conf.setMapOutputKeyClass(TextDatePair.class);
//	conf.setMapOutputValueClass(Text.class);
//
//	/* Reduce Configuration */
//	conf.setReducerClass(mapred.reduce.StageOneJoinStrategyHistoryJoinTransactionReducer.class);
//	conf.setOutputKeyClass(TextDatePair.class);
//	conf.setOutputValueClass(Text.class);
//
//	/* Partition and Compare Configuration */
//	conf.setPartitionerClass(DatePartitioner.class);
//	conf.setOutputKeyComparatorClass(TextDatePair.KeyComparator.class);
//	conf.setOutputValueGroupingComparator(TextDatePair.GroupComparator.class);
//
//	/* I/O Configuration */
//	conf.setOutputFormat(SequenceFileOutputFormat.class);
//	SequenceFileOutputFormat.setOutputPath(conf, outputStage2);
//
//	try
//	{
//	    JobClient.runJob(conf);
//	} 
//	catch (Exception e)
//	{
//	    e.printStackTrace();
//	    return 1;
//	}
//
//	conf = new JobConf(PaymentAnalysisWithObjects.class);
//	conf.setJobName("FinalAccountReducer");
//
//	/* Map Configuration */
//	conf.setInputFormat(SequenceFileInputFormat.class);
//	SequenceFileInputFormat.setInputPaths(conf, outputStage2);
//	conf.setMapperClass(IdentityMapper.class);
//
//	conf.setMapOutputKeyClass(TextDatePair.class);
//	conf.setMapOutputValueClass(Text.class);
//
//	/* Reduce Configuration */
//	conf.setReducerClass(mapred.reduce.FinalAccountReducer.class);
//	conf.setOutputKeyClass(Text.class);
//	conf.setOutputValueClass(Text.class);
//
//	/* Partition and Compare Configuration */
//	conf.setPartitionerClass(DatePartitioner.class);
//	conf.setOutputKeyComparatorClass(TextDatePair.KeyComparator.class);
//	conf.setOutputValueGroupingComparator(TextDatePair.GroupComparator.class);
//
//	/* I/O Configuration */
//	if (mode == 0)
//	{
//	    conf.setOutputFormat(SequenceFileOutputFormat.class);
//	    SequenceFileOutputFormat.setOutputPath(conf, outputStage3);
//	}
//	else
//	{
//	    conf.setOutputFormat(TextOutputFormat.class);
//	    TextOutputFormat.setOutputPath(conf, outputStage3);
//	}
//	try
//	{
//	    JobClient.runJob(conf);
//	} 
//	catch (Exception e)
//	{
//	    e.printStackTrace();
//	    return 1;
//	}
//	return 0;
//    }
//
    public static void printUsage()
    {
	System.out.println("Usage: PaymentAnalysisBetter <input path> <output path> [Remove existing output (true | false)]");
	System.exit(0);
    }

    public static void main(String[] args) throws Exception
    {
	if (args.length < 2)
	{
	    printUsage();
	}

	int exitCode = ToolRunner.run(new CustomerJoinAccountJob(), args);
	if(exitCode == 0)
	{
	    exitCode = ToolRunner.run(new AccountStatisticsJob(), args);
	}
	if(exitCode == 0)
	{
	    exitCode = ToolRunner.run(new PastAccountStatisticsJob(), args);
	}
	if(exitCode == 0)
	{
	    exitCode = ToolRunner.run(new FinalAccountJob(), args);
	}
	System.exit(exitCode);
    }
}
