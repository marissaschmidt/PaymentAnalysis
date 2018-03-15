package old;

import io.DateWritable;
import io.InvalidDateException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * A WritableComparable for Account Transactions. 
 * 
 * @author Marissa Hollingsworth
 */
public class Transaction extends EventWritable {

	/**
	 * The {@link NumberFormat} used to parse transaction amounts. 
	 */
	//public static final NumberFormat nf = DecimalFormat.getCurrencyInstance();
	public static final NumberFormat nf = DecimalFormat.getInstance();
	
	
	public static final String CHARGE = "Charge";
	public static final String ADJUSTMENT = "Adjustment";
	public static final String PAYMENT = "Payment";
	
	private DateWritable date;
	private DoubleWritable amount;
	private Text type;

	/**
	 * Creates a <code>Transaction</code> instance with default values. 
	 */
	public Transaction() {
		super();
		setAmount(0);
	}
	
	/**
	 * Creates a <code>Transaction</code> instance with specified values. 
	 * @param date The date of the Transaction.
	 * @param type The type of the Transaction.
	 * @param amount The amount of the Transaction.
	 */
	public Transaction(DateWritable date, Text type, DoubleWritable amount) {
		super(date, type);
		this.amount = amount;
	}
	
	/**
	 * Creates a <code>Transaction</code> instance with specified values. 
	 * @param date The date of the Transaction. Must be of the format "dd/mm/yyyy".
	 * @param type The type of the Transaction.
	 * @param amount The amount of the Transaction.
	 * @throws InvalidDateException
	 */
	public Transaction(String date, String type, String amount) throws InvalidDateException {
		super(new DateWritable(date), new Text(type));
		setAmount(amount);
	}

	/**
	 * Creates a <code>Transaction</code> instance with specified values. 
	 * @param date The date of the Transaction. Must be of the format "dd/mm/yyyy".
	 * @param type The type of the Transaction.
	 * @param amount The amount of the Transaction.
	 * @throws InvalidDateException
	 */
	public Transaction(String type, Date date, String amount) {
		super(date, type);
		setAmount(amount);
	}
	
	/**
	 * Copy constructor for <code>Transaction</code> objects.
	 * @param other The Transaction to be copied.
	 */
	public Transaction(EventWritable other) {
		super(other.getDate(), other.getType());
		if(other instanceof Transaction) {
			setAmount(((Transaction)other).getAmount());
		}
	}

	/**
	 * Copy constructor for <code>Transaction</code> objects.
	 * @param other The Transaction to be copied.
	 */
	public Transaction(Transaction other) {
		super(other.getDate(), other.getType());
		setAmount(other.getAmount());
	}



	/**
	 * Returns the amount of this <code>Transaction</code> as a <code>double</code>
	 * value.
	 * 
	 * @return the amount of this <code>Transaction</code>.
	 */
	public double getAmount() {
		return amount.get();
	}

	/**
	 * Set the amount of this <code>Transaction</code> .
	 * 
	 * @param amount
	 *            A <code>double</code> value.
	 */
	public void setAmount(double amount) {
		this.amount = new DoubleWritable(amount);
	}

	/**
	 * Set the amount of this <code>Transaction</code>.
	 * 
	 * @param amount
	 *            A <code>String</code> whose value should be parsed and used as
	 *            the double value. The string should be of the {@link Currency}
	 *            format. (ie. $0.00 for positive and ($0.00) for negative
	 *            values)
	 * @throws InvalidCurrencyException
	 * @throws ParseException
	 *             - if the beginning of the specified string cannot be parsed.
	 */
	public void setAmount(String amount) {
		Number value;
		try {
			value = nf.parse(amount);
			if (value instanceof Long)
				this.amount = new DoubleWritable(((Long) value).doubleValue());
			if (value instanceof Double)
				this.amount = new DoubleWritable((Double) value);
		} catch (ParseException e) {
			System.err.println("Invalid amount: " + amount);
			this.amount.set(0.0);
		}
	}

	/**
	 * Set the amount of this <code>Transaction</code>.
	 * 
	 * @param amount
	 *            A <code>DoubleWritable</code>.
	 */
	public void setAmount(DoubleWritable amount) {
		this.amount = amount;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		amount.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		amount.write(out);
	}

//	@Override
//	public int compareTo(EventWritable e) {
//		if(e instanceof Transaction) {
//			Transaction tw = (Transaction) e;
//			int cmp = super.compareTo(e);
//			if(cmp != 0)
//				return amount.compareTo(tw.amount);
//		}
//		return super.compareTo(e);
//	}
	
	@Override
	public int hashCode() {
		return super.hashCode() + amount.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Transaction) {
			Transaction tw = (Transaction) o;
			return super.equals(o) && amount.equals(tw.amount);
		}
		return false;
	}

	@Override
	public String toString() {
		DecimalFormat df = new DecimalFormat("#.##");
		return super.toString() + "amount[" + df.format(amount.get()) + "] ";
	}
}
