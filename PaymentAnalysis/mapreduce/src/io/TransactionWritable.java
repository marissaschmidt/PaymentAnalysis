package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TransactionWritable implements WritableComparable<TransactionWritable>
{
    private Text uniqueId;
    private EnumWritable<TransactionType> transactionType;
    private EnumWritable<StrategyType>	  strategyType;
    private DateWritable    	date;
    private DoubleWritable  	amount;

    public TransactionWritable(Text uniqueId)
    {
	this.setUniqueId(uniqueId);
	setTransactionType(TransactionType.UNKNOWN);
	setStrategyType(StrategyType.UNKNOWN);
	this.date = new DateWritable();
	this.amount = new DoubleWritable(0.0);
    }
    
    public TransactionWritable(Text uniqueId, TransactionType transactionType, 
	    StrategyType strategyType, DateWritable date, DoubleWritable amount)
    {
	this.setUniqueId(uniqueId);
	setTransactionType(transactionType);
	setStrategyType(strategyType);
	this.date = date;
	this.amount = amount;
    }

    /**
     * @param uniqueId the uniqueId to set
     */
    public void setUniqueId(Text uniqueId)
    {
	this.uniqueId = uniqueId;
    }

    /**
     * @return the uniqueId
     */
    public Text getUniqueId()
    {
	return uniqueId;
    }
    public TransactionType getTransactionType() throws IOException
    {
	return transactionType.get(TransactionType.class);
    }
    
    public void setTransactionType(TransactionType type)
    {
	this.transactionType = new EnumWritable<TransactionType>(type);
    }
    
    public StrategyType getStrategyType() throws IOException
    {
	return strategyType.get(StrategyType.class);
    }
    
    public void setStrategyType(StrategyType strategyType)
    {
	this.strategyType = new EnumWritable<StrategyType>(strategyType);
    }

    public double getAmount()
    {
	return amount.get();
    }

    public Date getDate()
    {
	return date.get();
    }
    public String toString()
    {
	return transactionType + "," + strategyType + "," + date + "," + amount;
    }

    @Override
    public int compareTo(TransactionWritable e)
    {
	int cmp = date.compareTo(e.date);
	//if (cmp == 0)
	//{
	//    cmp = type.compareTo(e.type);
	//}
	return cmp;
    }

    @Override
    public boolean equals(Object o)
    {
	if (o instanceof TransactionWritable)
	{
	    TransactionWritable tw = (TransactionWritable) o;
	    return (uniqueId.equals(tw.uniqueId) &&
		    date.equals(tw.date) && 
		    transactionType.equals(tw.transactionType) && 
		    strategyType.equals(tw.strategyType) && 
		    amount.equals(tw.amount));
	}
	return false;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
	uniqueId.write(out);
	transactionType.write(out);
	strategyType.write(out);
	date.write(out);
	amount.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
	uniqueId.readFields(in);
	transactionType.readFields(in);
	strategyType.readFields(in);
	date.readFields(in);
	amount.readFields(in);
    }



}
