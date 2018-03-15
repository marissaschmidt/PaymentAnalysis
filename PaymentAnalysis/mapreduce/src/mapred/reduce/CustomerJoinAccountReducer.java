package mapred.reduce;

import io.DateWritable;
import io.TextDatePair;
import io.TextPair;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * key: [AccountNumber,OpenDate] value: [Ssn, OpenDate, ZipCode3]
 */
@SuppressWarnings("deprecation")
public class CustomerJoinAccountReducer extends MapReduceBase
		implements Reducer<TextPair, Text, TextDatePair, Text> {

	TextDatePair outKey;
	Text outValue;

	Calendar calendar = new GregorianCalendar();

	@Override
	public void reduce(TextPair key, Iterator<Text> values,
			OutputCollector<TextDatePair, Text> output, Reporter reporter)
			throws IOException {

		String[] ssn_zip = (values.next()).toString().split(",");
		String[] account_date;

		while (values.hasNext()) {
			try {
				account_date = (values.next()).toString().split(",");
				calendar.setTime(DateWritable.df.parse(account_date[1]));
				calendar.add(Calendar.DAY_OF_MONTH, -1);

				outKey = new TextDatePair(account_date[0].toString(),
						calendar.getTime());
				outValue = new Text(ssn_zip[0] + "," + account_date[1]
						+ "," + ssn_zip[1]);
				output.collect(outKey, outValue);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
}