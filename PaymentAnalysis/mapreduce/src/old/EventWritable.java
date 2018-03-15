package old;

import io.DateWritable;
import io.InvalidDateException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * A WritableComparable for events. Each event has a date and type. 
 * 
 * @author Marissa Hollingsworth
 */
public class EventWritable implements WritableComparable<EventWritable> {

	/** The date the event occurred. */
	private DateWritable date;
	/** The specific type of event. */
	private Text type;
	
	/**
	 * Creates an <code>EventWritable</code> instance.
	 */
	public EventWritable() {
		setDate(new DateWritable());
		setType(new Text());
	}
	
	/**
	 * Creates an <code>EventWritable</code> instance with the specified date and type.
	 * @param date The date of the event.
	 * @param type The type of the event.
	 */
	public EventWritable(DateWritable date, Text type) {
		setDate(date);
		setType(type);
	}
	
	public EventWritable(String date, String type) throws InvalidDateException {
		setDate(new DateWritable(date));
		setType(new Text(type));
	}
	
	public EventWritable(Date date, String type) {
		setDate(new DateWritable(date));
		setType(new Text(type));
	}
	
	/**
	 * Copy constructor for <code>EventWritable</code> objects.
	 * @param other The <code>EventWritable</code> to be copied.
	 */
	public EventWritable(EventWritable other) {
		setDate(other.date);
		setType(other.type);
	}
	
	/**
	 * Returns the date of this event.
	 * @return the date.
	 */
	public DateWritable getDate() {
		return date;
	}

	/**
	 * Sets the date this event occurred.
	 * @param date The date.
	 */
	public void setDate(DateWritable date) {
		this.date = date;
	}
	
	/**
	 * Sets the date this event occurred.
	 * @param date The date.
	 */
	public void setDate(String date) throws InvalidDateException {
		this.date.set(date);
	}

	/**
	 * Returns the type of this event.
	 * @return the type.
	 */
	public Text getType() {
		return type;
	}

	/**
	 * Sets the type of this event.
	 * @param type A <code>Text</code> object representing the event type.
	 */
	public void setType(Text type) {
		this.type = type;
	}
	
	/**
	 * Sets the type of this event.
	 * @param type A <code>String</code> object representing the event type.
	 */
	public void setType(String type) {
		this.type.set(type);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		date.readFields(in);
		type.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		date.write(out);
		type.write(out);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof EventWritable) {
			EventWritable tw = (EventWritable) o;
			return date.equals(tw.date)	&& type.equals(tw.type);
		}
		return false;
	}
	
	@Override
	public int compareTo(EventWritable e) {
		int cmp = date.compareTo(e.date);
		if (cmp != 0)
			return cmp;
		return -(type.compareTo(e.type));
	}
	
	@Override
	public int hashCode() {
		return date.hashCode() * 163 + type.hashCode();
	}
	
	@Override
	public String toString() {
		return "date[" + date + "] type[" + type + "] ";
	}
}
