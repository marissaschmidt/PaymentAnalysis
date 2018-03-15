package mapred.map;

import io.InvalidDateException;
import io.TextDatePair;
import io.TransactionType;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [AccountNumber,date] value: [type, amount, date]
 */
@SuppressWarnings("deprecation")
public class TransactionMapper extends MapReduceBase implements
		Mapper<Object, Text, TextDatePair, Text> {

	public static final int COL_ACCOUNT_ID = 0;
	public static final int COL_DATE = 1;
	public static final int COL_AMOUNT = 2;
	public static final int COL_TYPE = 3;

	private TextDatePair key = new TextDatePair();
	private Text value = new Text();

	public void map(Object offset, Text input,
			OutputCollector<TextDatePair, Text> output, Reporter reporter)
			throws IOException {

		String[] parts = input.toString().split(",");

		if (parts.length > 4) {
			parts[2] = (parts[2] + parts[3]).replaceAll("[ \"]", "");
			parts[3] = parts[4];
		}

		String accountNumber = parts[COL_ACCOUNT_ID].trim();
		String date = parts[COL_DATE].trim();
		String amount = parts[COL_AMOUNT].trim();
		int type = TransactionType.getByName(parts[COL_TYPE].trim()).getOffset();

		try {
			key.set(accountNumber, date);
			value.set(type + "," + date + "," + amount);
		} catch (InvalidDateException e) {
			System.out.println(e.getMessage());
		}

		output.collect(key, value);
	}
}