package mapred.reduce;

import io.InvalidDateException;
import io.StrategyType;
import io.TextDatePair;
import io.TransactionType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Input 
 * ===================================== 
 * key: [AccountNumber, 0]
 * value: [Ssn, ZipCode3, OpenDate]
 * 
 * key: [AccountNumber, 1] 
 * value: [strategyType, date]
 * 
 * key: [AccountNumber, 2] 
 * value: [transactionType, date, amount]
 * ======================================
 * 
 * Output 
 * ==================================================================
 * key: [Ssn] 
 * value: [AccountNumber, ZipCode3, OpenDate, GoodStartDate, BadStartDate]
 * ===================================================================
 */
@SuppressWarnings("deprecation")
public class StageOneJoinStrategyHistoryJoinTransactionReducer extends
		MapReduceBase implements
		Reducer<TextDatePair, Text, TextDatePair, Text> {

	private static final int COL_TYPE = 0;
	private static final int COL_DATE = 1;
	private static final int COL_AMOUNT = 2;

	TextDatePair outKey;
	Text outValue;

	@Override
	public void reduce(TextDatePair key, Iterator<Text> values,
			OutputCollector<TextDatePair, Text> output, Reporter reporter)
			throws IOException {

		/* The account number is the key */
		String accountNumber = key.getFirst().toString();

		/* We expect [Ssn, ZipCode3, OpenDate] first */
		String value = (values.next()).toString();
		String[] next = value.split(",");

		System.out.println("-----------------------------");
		System.out.println(key.getFirst());
		System.out.println("-----------------------------");

		String ssn = next[COL_TYPE];
		String openDate = next[COL_DATE];
		String zipCode3 = next[COL_AMOUNT];

		double[][] transactions = new double[3][8];
		Arrays.fill(transactions[TransactionType.CHARGE.getOffset()], 0);
		Arrays.fill(transactions[TransactionType.ADJUSTMENT.getOffset()], 0);
		Arrays.fill(transactions[TransactionType.PAYMENT.getOffset()], 0);

		int type;
		int strategy = 0;
		while (values.hasNext()) {
			value = (values.next()).toString();
			next = value.split(",");

			type = Integer.parseInt(next[COL_TYPE]);
			System.out.println(value);
			
			if (type >= StrategyType.STRATEGY_OFFSET) {
				if ((type % StrategyType.STRATEGY_OFFSET) > strategy)
					strategy = type % StrategyType.STRATEGY_OFFSET;
			} else {
				transactions[type][strategy] += Double
						.parseDouble(next[COL_AMOUNT]);
				
				// TODO dont write bad charges or adjustments
				try {
					outKey = new TextDatePair(ssn, next[COL_DATE]);
					outValue = new Text(type + "," + strategy + ","
							+ next[COL_AMOUNT]);
					output.collect(outKey, outValue);
				} catch (InvalidDateException e) {
					System.out.println(e.getMessage());
				}
			}
		}

		System.out.println("-----------------------------");

		double goodCharges = 0.0;
		double goodAdjustments = 0.0;

		for (int i = 0; i < 4; i++) {
			goodCharges += transactions[TransactionType.CHARGE.getOffset()][i];
			goodAdjustments += transactions[TransactionType.ADJUSTMENT.getOffset()][i];
		}

		double goodPayment30 = transactions[TransactionType.PAYMENT.getOffset()][0];
		double goodPayment60 = goodPayment30
				+ transactions[TransactionType.PAYMENT.getOffset()][1];
		double goodPayment90 = goodPayment60
				+ transactions[TransactionType.PAYMENT.getOffset()][2];
		double goodPaymentTotal = goodPayment90
				+ transactions[TransactionType.PAYMENT.getOffset()][3];

		double badDebtTransferBalanace = goodCharges + goodAdjustments
				+ goodPaymentTotal;

		double totalCharges = goodCharges;
		double totalAdjustments = goodAdjustments;

		for (int i = 4; i < 8; i++) {
			totalCharges += transactions[TransactionType.CHARGE.getOffset()][i];
			totalAdjustments += transactions[TransactionType.ADJUSTMENT.getOffset()][i];
		}

		double adjustedTotalCharges = totalCharges + totalAdjustments;

		double badPayment30 = transactions[TransactionType.PAYMENT.getOffset()][4];
		double badPayment60 = badPayment30 + transactions[TransactionType.PAYMENT.getOffset()][5];
		double badPayment90 = badPayment60 + transactions[TransactionType.PAYMENT.getOffset()][6];
		double badPaymentTotal = badPayment90
				+ transactions[TransactionType.PAYMENT.getOffset()][7];

		String account = String
				.format("%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",
						accountNumber, openDate, zipCode3, totalCharges,
						totalAdjustments, adjustedTotalCharges,
						-goodPayment30, -goodPayment60, -goodPayment90,
						badDebtTransferBalanace, -badPayment30,
						-badPayment60, -badPayment90);

		/*
		 * We only want to output this data once (not append to every
		 * transaction)
		 */
		try {
			outKey = new TextDatePair(ssn, openDate);
			outValue = new Text(account);
			output.collect(outKey, outValue);
		} catch (InvalidDateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}