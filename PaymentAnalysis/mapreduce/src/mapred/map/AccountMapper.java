package mapred.map;

import io.TextPair;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [CustomerNumber,1] value: [AccountNumber, OpenDate]
 */
@SuppressWarnings("deprecation")
public class AccountMapper extends MapReduceBase implements
	Mapper<Object, Text, TextPair, Text>
{
    public static final int COL_ACCOUNT_NUM  = 0;
    public static final int COL_OPEN_DATE    = 1;
    public static final int COL_CUSTOMER_NUM = 2;

    private TextPair key = new TextPair();
    private Text value;

    public void map(Object offset, Text input,
	    OutputCollector<TextPair, Text> output, Reporter reporter)
	    throws IOException
    {

	String[] parts = (input.toString()).split(",");

	String accountNumber = parts[COL_ACCOUNT_NUM].trim();
	String openDate = parts[COL_OPEN_DATE].trim();
	String customerNumber = parts[COL_CUSTOMER_NUM].trim();

	key.set(customerNumber, "1");
	value = new Text(accountNumber + "," + openDate);
	output.collect(key, value);
    }
}
