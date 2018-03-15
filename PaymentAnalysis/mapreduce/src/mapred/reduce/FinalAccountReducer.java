package mapred.reduce;

import io.AccountWritable;
import io.StrategyType;
import io.TextDatePair;
import io.TransactionType;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class FinalAccountReducer extends MapReduceBase implements
		Reducer<TextDatePair, Text, Text, Text> {

	Text outKey;
	Text outValue;

	@Override
	public void reduce(TextDatePair key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		int previousAccounts = 0;
		double totalGoodCharges = 0.0;
		double totalGoodAdjustments = 0.0;
		double totalGoodPayments = 0.0;
		double totalBadPayments = 0.0;

		String value;
		String[] next;

		String accountNumber;
		int type;
		int strategy;
		double amount;

		while (values.hasNext()) {
			value = (values.next()).toString();
			next = value.split(",");

			try {
				type = Integer.parseInt(next[0]);
			} catch (NumberFormatException e) {
				type = TransactionType.PAYMENT.getOffset() + 1;
			}
			if (type > TransactionType.PAYMENT.getOffset()) {
				accountNumber = next[0];
				outKey = new Text(accountNumber);
				outValue = new Text(value
						+ String.format(",%d,%.2f,%.2f,%.2f,%.2f",
								previousAccounts, totalGoodCharges,
								totalGoodAdjustments, totalGoodPayments,
								totalBadPayments));
				output.collect(outKey, outValue);
				previousAccounts++;
			}

			else {
				type = Integer.parseInt(next[0]);
				strategy = Integer.parseInt(next[1]);
				amount = Double.parseDouble(next[2]);

				if (strategy < (StrategyType.BAD_DEBT.getNumberOfDays() % StrategyType.STRATEGY_OFFSET)) {
					if (type == TransactionType.CHARGE.getOffset())
						totalGoodCharges += amount;
					else if (type == TransactionType.ADJUSTMENT.getOffset())
						totalGoodAdjustments += amount;
					else if (type == TransactionType.PAYMENT.getOffset())
						totalGoodPayments += amount;
				} else if (type == TransactionType.PAYMENT.getOffset()) {
					totalBadPayments += amount;
				}
			}
		}
	}
}
