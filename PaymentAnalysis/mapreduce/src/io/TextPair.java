package io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class stores a text, integer, and text value. It was created to 
 * use in the {@link InvertedIndex} project in order to sort values 
 * based on the integer and second text value. The tuple is created as 
 * followed:  
 * 
 * TextPair(String word, int count, String documentId);
 * 
 * This class includes a {@link KeyComparator} class that sorts the objects
 * by word, then count, then documentId and a {@link GroupComparator} class
 * that sorts the objects by word only.
 * 
 * @author Shannon Heck
 * @author Marissa Hollingsworth
 * @version Fall 2010 - CompSci 530: Parallel Computing
 */
public class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	/**
	 * Construct an empty {@link TextPair} object.
	 */
	public TextPair() {
		set(new Text(), new Text());
	}

	/**
	 * Construct from {@link String}, {@code int}, and {@link String} objects.
	 * @param first the first {@code String} object.
	 * @param second the {@code int} object.
	 * @param second the second {@code String} object.
	 */
	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}

	/**
	 * Construct from {@link Text}, {@link IntWritable}, and {@link Text} objects.
	 * @param first the first {@code Text} object.
	 * @param second the {@code IntWritable} object.
	 * @param second the second {@code Text} object.
	 */
	public TextPair(Text first, Text second) {
		set(first, second);
	}
	
	public TextPair(TextPair other) {
		this.first = new Text(other.first);
		this.second = new Text(other.second);
	}
	/**
	 * Set to contain the contents of {@link Text}, {@link IntWritable}, and {@link Text} objects.
	 * @param first the first {@code Text} object.
	 * @param second the {@code IntWritable} object.
	 * @param second the second {@code Text} object.
	 */
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	/**
	 * Set to contain the contents of {@link Text}, {@link IntWritable}, and {@link Text} objects.
	 * @param first the first {@code Text} object.
	 * @param second the {@code IntWritable} object.
	 * @param second the second {@code Text} object.
	 */
	public void set(String first, String second) {
		set(new Text(first), new Text(second));
	}
	/**
	 * Get the first field of this {@link TextPair}.
	 * @return Text the second field of the object
	 */
	public Text getFirst() {
		return first;
	}

	/**
	 * Get the second field of this {@link TextPair}.
	 * @return Text the second field of the object
	 */
	public Text getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	public String toString() {
		return first + "," + second;
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode() * 163;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tuple = (TextPair) o;
			return first.equals(tuple.first) && second.equals(tuple.second);
		}
		return false;
	}

	@Override
	public int compareTo(TextPair that) {
		int cmp = first.compareTo(that.first);
		if (cmp == 0) /* If words are the same, compare counts */
			cmp = (second.compareTo(that.second));
		return cmp;
	}

	/** A WritableComparator optimized for TextPair keys. */
	public static class KeyComparator extends WritableComparator {

		/**
		 * A Comparator for <code>WritableComparable</code><
		 * <code>TextPair</code>>s.
		 */
		public KeyComparator() {
			super(TextPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			if (w1 instanceof TextPair
					&& w2 instanceof TextPair) {
				TextPair t1 = (TextPair) w1;
				TextPair t2 = (TextPair) w2;
				return t1.compareTo(t2);
			}
			return super.compare(w1, w2);
		}
	}

	/** A WritableComparator optimized for TextPair keys. */
	public static class GroupComparator extends WritableComparator {

		/**
		 * A Comparator for <code>WritableComparable</code><
		 * <code>TextPair</code>>s.
		 */
		protected GroupComparator() {
			super(TextPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			if (w1 instanceof TextPair
					&& w2 instanceof TextPair) {
				TextPair t1 = (TextPair) w1;
				TextPair t2 = (TextPair) w2;

				return t1.getFirst().compareTo(t2.getFirst());
			}
			return super.compare(w1, w2);
		}
	}
}
