package mapred.map;

import io.TextPair;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [CustomerNumber,0] value: [Ssn, ZipCode3]
 */
@SuppressWarnings("deprecation")
public class CustomerMapper extends MapReduceBase implements
		Mapper<Object, Text, TextPair, Text>
{
    public static final int COL_CUSTOMER_NUM = 0;
    public static final int COL_FIRST_NAME   = 1;
    public static final int COL_LAST_NAME    = 2;
    public static final int COL_SSN	     = 3;
    public static final int COL_ZIP_CODE_3   = 4;

    private static final Text ZERO = new Text("0");

    private TextPair key = new TextPair();
    private Text value;

    @Override
    public void map(Object offset, Text input, OutputCollector<TextPair, Text> output,
	    Reporter reporter) throws IOException
    {
	String[] parts = (input.toString()).split(",");

	String customerNumber = parts[COL_CUSTOMER_NUM].trim();
	String ssn = parts[COL_SSN].trim();
	String zipCode3 = parts[COL_ZIP_CODE_3].trim();

	key.set(new Text(customerNumber), ZERO);
	value = new Text(ssn + "," + zipCode3);

	output.collect(key, value);
    }
}
