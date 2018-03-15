package old;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	public IntArrayWritable(IntWritable[] values) {
		super(IntWritable.class);
		for (int i = 0; i < values.length; i++)
			values[i] = new IntWritable();
		this.set(values);
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		int length = get().length;
		Writable[] values = get();

		for (int i = 0; i < length; i++) {
			builder.append("[" + ((IntWritable)values[i]).get() + "]");
		}

		return builder.toString();
	}
}