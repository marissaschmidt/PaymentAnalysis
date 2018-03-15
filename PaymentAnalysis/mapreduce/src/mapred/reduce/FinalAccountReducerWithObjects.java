package mapred.reduce;

import io.AccountStatisticsWritable;
import io.AccountWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class FinalAccountReducerWithObjects extends MapReduceBase implements Reducer<Text, AccountWritable, Text, AccountWritable> 
{
    @Override
    public void reduce(Text key, Iterator<AccountWritable> values, OutputCollector<Text, AccountWritable> output, Reporter reporter) throws IOException 
    {
	AccountWritable outValue = new AccountWritable();
	
	while(values.hasNext())
	{
	    AccountWritable account = values.next();
	    AccountStatisticsWritable statistics = account.getStatistics();
	    
	    if(statistics == null) // this entry has account info
	    {
		outValue.setSsn(account.getSsn());
		outValue.setCustomerNumber(account.getCustomerNumber());
		outValue.setAccountNumber(account.getAccountNumber());
		outValue.setOpenDate(account.getOpenDate());
		outValue.setZipCode3(account.getZipCode3());
	    }
	    else // this entry has account statistics
	    {
		outValue.setStatistics(statistics);
	    }
	}
	output.collect(key, outValue);
    }
}
