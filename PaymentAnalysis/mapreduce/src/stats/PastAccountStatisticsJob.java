package stats;

import io.AccountWritable;
import mapred.reduce.PastAccountStatisticsReducerWithObjects;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;

@SuppressWarnings("deprecation")
public class PastAccountStatisticsJob extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
	final Path customerJoinAccountInput = new Path(args[0] + Path.SEPARATOR_CHAR + "CustomerJoinAccount" + Path.SEPARATOR_CHAR + "TransactionWritable");
	final Path accountStatisticsInput = new Path(args[0] + Path.SEPARATOR_CHAR + "AccountStatistics" + Path.SEPARATOR_CHAR + "AccountWritable");
	
	final Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "PastAccountStatistics");
	
	JobConf job = new JobConf(PastAccountStatisticsJob.class);
	job.setJobName("PastAccountStatisticsJob");

	/* Map Configuration */
	MultipleInputs.addInputPath(job, customerJoinAccountInput, SequenceFileInputFormat.class, IdentityMapper.class);
	MultipleInputs.addInputPath(job, accountStatisticsInput, SequenceFileInputFormat.class, IdentityMapper.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(AccountWritable.class);

	/* Reduce Configuration */
	job.setReducerClass(PastAccountStatisticsReducerWithObjects.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(AccountWritable.class);

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
