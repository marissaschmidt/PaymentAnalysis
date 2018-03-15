package mapred.map;

import io.DateWritable;
import io.StrategyType;
import io.TextDatePair;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [AccountNumber,date] value: [GoodStanding]
 * 
 * key: [AccountNumber,date] value: [BadDebt]
 */
@SuppressWarnings("deprecation")
public class StrategyHistoryMapper extends MapReduceBase implements
		Mapper<Object, Text, TextDatePair, Text> {

	public static final int COL_ACCOUNT_NUMBER = 0;
	public static final int COL_NAME = 1;
	public static final int COL_DATE = 2;

	private TextDatePair key = new TextDatePair();
	private Text value = new Text();

	private Calendar calendar = new GregorianCalendar();

	public void map(Object offset, Text input,
			OutputCollector<TextDatePair, Text> output, Reporter reporter)
			throws IOException {

		String[] parts = (input.toString()).split(",");

		String accountNumber = parts[COL_ACCOUNT_NUMBER].trim();
		String name = parts[COL_NAME].trim();
		int type = StrategyType.getByName(name).getNumberOfDays();

		try {
			calendar.setTime(DateWritable.df.parse(parts[COL_DATE].trim()));

			key.set(accountNumber, calendar.getTime());
			value.set(Integer.toString(type));
			output.collect(key, value);

			calendar.add(Calendar.DAY_OF_MONTH, 30);
			key.set(accountNumber, calendar.getTime());
			value.set(Integer.toString(type + 1));
			output.collect(key, value);

			calendar.add(Calendar.DAY_OF_MONTH, 30);
			key.set(accountNumber, calendar.getTime());
			value.set(Integer.toString(type + 2));
			output.collect(key, value);

			calendar.add(Calendar.DAY_OF_MONTH, 30);
			key.set(accountNumber, calendar.getTime());
			value.set(Integer.toString(type + 3));
			output.collect(key, value);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}