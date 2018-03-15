package io;

import java.text.DecimalFormat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable
{

    public DoubleArrayWritable()
    {
	super(DoubleWritable.class);
    }

    public DoubleArrayWritable(DoubleWritable[] values)
    {
	super(DoubleWritable.class);
	for (int i = 0; i < values.length; i++)
	    values[i] = new DoubleWritable();
	this.set(values);
    }

    public DoubleArrayWritable(double[] values)
    {
	super(DoubleWritable.class);

	DoubleWritable[] writables = new DoubleWritable[values.length];

	for (int i = 0; i < values.length; i++)
	{
	    writables[i] = new DoubleWritable(values[i]);
	}
	this.set(writables);
    }
    
    public void set(double[] values)
    {
	DoubleWritable[] writables = new DoubleWritable[values.length];

	for (int i = 0; i < values.length; i++)
	{
	    writables[i] = new DoubleWritable(values[i]);
	}
	this.set(writables);
    }
    

    public String toString()
    {
	StringBuilder builder = new StringBuilder();
	DecimalFormat df = new DecimalFormat("#.##");
	int length = get().length;
	Writable[] values = get();

	for (int i = 0; i < length; i++)
	{
	    builder.append("[" + df.format(((DoubleWritable) values[i]).get())
		    + "]");
	}

	return builder.toString();
    }
}
