package mapred.map;

import io.AccountWritable;
import io.DateWritable;
import io.TextPair;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [CustomerNumber,0] value: [Ssn, ZipCode3]
 */
@SuppressWarnings("deprecation")
public class CustomerMapperWithObjects extends MapReduceBase implements
		Mapper<Object, Text, TextPair, AccountWritable> 
{
    public static final int COL_CUSTOMER_NUM = 0;
    public static final int COL_FIRST_NAME = 1;
    public static final int COL_LAST_NAME = 2;
    public static final int COL_SSN = 3;
    public static final int COL_ZIP_CODE_3 = 4;	
    
    private static final Text accountNumber = new Text();
    private static final DateWritable openDate = new DateWritable();
    private static final Text ZERO = new Text("0");

    private TextPair key = new TextPair();
    private AccountWritable value;

    /**
     * Processes each Customer entity and outputs a new {@link AccountWritable}
     * object with the {@code customerNumber}, {@code zipCode3}, and 
     * {@code ssn} fields set to the values from the current entity 
     * and the {@code accountNumber} and {@code openDate} fields set to {@code ""}.
     * <p>
     * key: [CustomerNumber,0] 
     * <br>
     * value: {@code new AccountWritable(CustomerNumber, "", Ssn, "", ZipCode3)}
     * </p>
     */
    @Override
    public void map(Object offset, Text input, OutputCollector<TextPair, AccountWritable> output,
	    Reporter reporter) throws IOException 
    {
	String[] parts = (input.toString()).split(",");

	Text customerNumber = new Text(parts[COL_CUSTOMER_NUM].trim());
	Text ssn = new Text(parts[COL_SSN].trim());
	IntWritable zipCode3 = new IntWritable(Integer.parseInt(parts[COL_ZIP_CODE_3].trim()));

	key.set(customerNumber, ZERO);
	value = new AccountWritable(ssn, customerNumber, accountNumber, openDate, zipCode3);

	output.collect(key, value);
    }
}
