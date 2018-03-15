package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A WritableComparable for <code>Date</code> objects.
 * 
 * @author Marissa Hollingsworth
 */
public class DateWritable implements WritableComparable<DateWritable>
{

    /**
     * The {@link DateFormat} used to parse dates.
     */
    // private static DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
    public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    private Date	     date;

    /**
     * Creates a <code>null</code> <code>DateWritable</code> instance.
     */
    public DateWritable()
    {
	date = null;
    }

    /**
     * Creates a new <code>DateWritable</code> instance from the specified
     * <code>Date</code> object.
     * 
     * @param date
     *            The date.
     */
    public DateWritable(Date date)
    {
	set(date);
    }

    /**
     * Creates a new <code>DateWritable</code> instance from the specified
     * <code>String</code> object.
     * 
     * @param date
     *            The date to be parsed. Must be of the format "dd/mm/yyyy".
     * @throws InvalidDateException
     */
    public DateWritable(String date) throws InvalidDateException
    {
	set(date);
    }

    /**
     * Creates a copy of the <code>DateWritable</code> object.
     * 
     * @param date
     *            The <code>DateWritable</code> to be copied.
     */
    public DateWritable(DateWritable date)
    {
	this.date = date.get();
    }

    /**
     * Sets the date of this <code>DateWritable</code> object.
     * 
     * @param date
     *            The date to be parsed. Must be of the format "dd/mm/yyyy".
     * @throws InvalidDateException
     */
    public void set(String date) throws InvalidDateException
    {
	try
	{
	    this.date = df.parse(date);
	} catch (ParseException e)
	{
	    throw new InvalidDateException("Unparseable date:" + date);
	}
    }

    /**
     * Sets the date of this <code>DateWritable</code> object.
     * 
     * @param date
     *            The date.
     */
    public void set(Date date)
    {
	this.date = date;
    }

    /**
     * Returns the date of this <code>DateWritable</code>.
     * 
     * @return The <code>Date</code>.
     */
    public Date get()
    {
	return date;
    }

    public void add(int calendarField, int amount)
    {
	Calendar calendar = new GregorianCalendar();
	calendar.setTime(date);
	calendar.add(calendarField, amount);
//	calendar.add(Calendar.DAY_OF_MONTH, -1);
    }
    
    public static DateWritable getAdjustedDate(DateWritable date, int calendarField, int amount)
    {
	Calendar calendar = new GregorianCalendar();
	calendar.setTime(date.get());
	calendar.add(calendarField, amount);
	return new DateWritable(calendar.getTime());
    }
    
    public String toString()
    {
	if (date == null)
	    return null;
	return df.format(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
	LongWritable time = new LongWritable();
	time.readFields(in);
	if (time.get() == 0)
	    date = null;
	else
	    date = new Date(time.get());
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
	LongWritable time = new LongWritable(0);
	if (date != null)
	    time.set(date.getTime());
	time.write(out);
    }

    @Override
    public int hashCode()
    {
	return date.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
	if (o instanceof DateWritable)
	{
	    DateWritable d = (DateWritable) o;
	    return date.equals(d.date);
	}
	return false;
    }

    @Override
    public int compareTo(DateWritable d)
    {
	return date.compareTo(d.date);
    }
}
