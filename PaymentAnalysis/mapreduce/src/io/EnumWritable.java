package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class EnumWritable<E extends Enum<E>> implements Writable  
{
    private byte storage;

    public EnumWritable() 
    {
	storage = 0;
    }

    public EnumWritable(Enum<E> value) 
    {
	set(value);
    }

    public <T extends Enum<E>> E get(Class<E> enumType) throws IOException 
    {
	for (E type : enumType.getEnumConstants()) 
	{
	    if (storage == type.ordinal())
		return type;
	}
	return null;
    }

    public void set(Enum<E> e) 
    {
	storage = (byte) e.ordinal();
    }

    public void readFields(DataInput in) throws IOException 
    {
	storage = in.readByte();
    }

    public void write(DataOutput out) throws IOException 
    {
	out.write(storage);
    }

    @Override
    public String toString() 
    {
	return Integer.toString(storage);
    }

    @Override
    public boolean equals(Object obj) 
    {
	if (!(obj instanceof EnumWritable)) 
	{
	    return super.equals(obj);
	}
	
	EnumWritable<?> that = (EnumWritable<?>) obj;
	return this.storage == that.storage;
    }

    @Override
    public int hashCode() 
    {
	return storage;
    }
}
