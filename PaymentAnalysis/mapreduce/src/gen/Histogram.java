package gen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

/**
 * A {@link WritableComparable} that represents a <code>Histogram</code>.
 * 
 * A <code>Histogram</code> is used to store the distribution
 * of int values to be used for statistical models. For example, each 
 * histogram has a form like:
 * 
 * 0 1 2 3 ... n	where n is the value and x is the count.
 * 4 0 5 5 ... x
 *  
 * @author Marissa Hollingsworth
 *
 */
public class Histogram implements WritableComparable<Histogram> {

	private HistogramType key;
	private int[] values;
	
	public Histogram() {
		this(HistogramType.DEFAULT, new Integer[0]);
	}
	
	public Histogram(Histogram other) {
		this.key = other.key;
		values = Arrays.copyOf(other.values, other.values.length);
	}
	
	public Histogram(HistogramType key) {
		this(key, new Integer[0]);
	}
	
	public Histogram(int key) {
		this(HistogramType.getType(key), new Integer[0]);
	}
	
	
	public Histogram(HistogramType key, Integer[] array) {
		this.key = key;
		
		values = new int[array.length];
		for(int i = 0; i < array.length; i++)
			values[i] = array[i].intValue();
	}
	
	/**
	 * Returns the array of values.
	 * @return An array of histogram values.
	 */
	public int[] getValues() {
		return values;
	}
	
	/**
	 * Returns the key for the Histogram. 
	 * @return {@link HistogramType} of this Histogram.
	 */
	public HistogramType getKey() {
		return key;
	}
	
	/**
	 * Sets the key for the Histogram.
	 * @param key The {@link HistogramType} of this Histogram. 
	 */
	public void setKey(HistogramType key) {
		this.key = key;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		/* read the key */
		key = HistogramType.getType(in.readInt());
		
		/* read the values */
		int size = in.readInt();
		values = new int[size];          
	    for (int i = 0; i < size; i++) {
	    	values[i] = in.readInt();                          // store it in values
	    }
	}

	@Override
	public void write(DataOutput out) throws IOException {
	    /* write the key */
		out.writeInt(key.ordinal());

		/* write the values */
		int size = values.length;
		out.writeInt(size);
	    for (int i = 0; i < size; i++) {
	      out.writeInt(values[i]);
	    }
	}

	@Override
	public int compareTo(Histogram o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		int[] temp = new int[15];
		builder.append("\n");
		builder.append(key.getName() + ",");		
		for(int i = 0; i < values.length; i++) {
			builder.append(values[i] + ",");
			temp[values[i]]++;
		}
		builder.append("\n");
		builder.append("value,");
		for(int i = 0; i < temp.length; i++) {
			builder.append(i + ",");
		}
		builder.append("\n");
		builder.append("count,");
		for(int i = 0; i < temp.length; i++) {
			builder.append(temp[i] + ",");
		}
		builder.append("\n");
		return builder.toString();
	}

}
