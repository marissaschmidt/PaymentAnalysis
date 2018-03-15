package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * A WritableComparable for Account objects.
 * 
 * @author Marissa Hollingsworth
 */
public class AccountWritable implements WritableComparable<AccountWritable>
{
    private Text	      ssn;
    private Text	      customerNumber;
    private Text	      accountNumber;
    private DateWritable      openDate;
    private IntWritable       zipCode3;
   
    private AccountStatisticsWritable statistics;

    /**
     * Creates a new <code>Account</code> instance with initial values of 0 for
     * all variables.
     */
    public AccountWritable()
    {
	ssn = new Text();
	customerNumber = new Text();
	accountNumber = new Text();
	openDate = new DateWritable();
	zipCode3 = new IntWritable(0);
	
	statistics = null;
    }

    /**
     * Creates a new <code>Account</code> instance with initial values of 0 for
     * all variables except statistics.
     */
    public AccountWritable(AccountStatisticsWritable statistics)
    {
	ssn = new Text();
	customerNumber = new Text();
	accountNumber = new Text();
	openDate = new DateWritable();
	zipCode3 = new IntWritable(0);
	
	this.statistics = statistics;
    }
    
    /**
     * Creates a new <code>Account</code> instance with the specified values.
     * All other values are initialized to 0.
     * 
     * @param accountNumber
     *            The unique account identifier.
     * @param openDate
     *            The date the account was opened.
     * @param zipCode
     *            The first 3 digits of the customer's zip code.
     */
    public AccountWritable(Text ssn, Text customerNumber, Text accountNumber, 
	    DateWritable openDate, IntWritable zipCode)
    {
	this.ssn = ssn;
	this.customerNumber = customerNumber;
	this.accountNumber = accountNumber;
	
	this.openDate = openDate;
	this.zipCode3 = zipCode;
	
	this.statistics = null;
    }
    /**
     * Creates a new <code>Account</code> instance with the specified values.
     * All other values are initialized to 0.
     * 
     * @param accountNumber
     *            The unique account identifier.
     * @param openDate
     *            The date the account was opened.
     * @param zipCode
     *            The first 3 digits of the customer's zip code.
     * @throws InvalidDateException 
     */
//    public AccountWritable(String accountNumber, String openDate, int zipCode3, String ssn) throws InvalidDateException
//    {
//	this.accountNumber = new Text(accountNumber);
//	this.openDate = new DateWritable(openDate);
//	this.zipCode3 = new IntWritable(zipCode3);
//	this.ssn = new Text(ssn);
//	this.customerNumber = new Text();
//	this.statistics = null;
//    }
    
    /**
     * Creates a new <code>Account</code> instance with the specified values.
     * All other values are initialized to 0.
     * 
     * @param accountNumber
     *            The unique account identifier.
     * @param openDate
     *            The date the account was opened.
     * @param zipCode
     *            The first 3 digits of the customer's zip code.
     */
//    public AccountWritable(Text accountNumber, DateWritable openDate, IntWritable zipCode3, Text ssn)
//    {
//	this.accountNumber = accountNumber;
//	this.openDate = openDate;
//	this.zipCode3 = zipCode3;
//	this.ssn = ssn;
//	this.customerNumber = new Text();
//	this.statistics = null;
//    }

    /**
     * Sets the unique account identifier for this account.
     * 
     * @param accountNumber
     *            The unique account identifier.
     */
    public void setAccountNumber(Text accountNumber)
    {
	this.accountNumber = accountNumber;
    }
    
    /**
     * Returns the unique account identifier of this account.
     * 
     * @return the account number.
     */
    public Text getAccountNumber()
    {
	return accountNumber;
    }

    /**
     * Sets the unique account identifier for this account.
     * 
     * @param accountNumber
     *            The unique account identifier.
     */
    public void setAccountNumber(String accountNumber)
    {
	this.accountNumber = new Text(accountNumber);
    }

    /**
     * Returns the date this account was opened.
     * 
     * @return the date.
     */
    public DateWritable getOpenDate()
    {
	return openDate;
    }

    /**
     * Sets the date this account was opened.
     * 
     * @param openDate
     *            The date the account was opened.
     */
    public void setOpenDate(DateWritable openDate)
    {
	this.openDate = openDate;
    }

    /**
     * Sets the date this account was opened.
     * 
     * @param openDate
     *            The date the account was opened.
     */
    public void setOpenDate(Date date)
    {
	this.openDate.set(date);
    }

    /**
     * Sets the date this account was opened.
     * 
     * @param openDate
     *            The date the account was opened. String should be of the
     *            format "dd/mm/yyyy"
     * @throws InvalidDateException
     */
    public void setOpenDate(String openDate) throws InvalidDateException
    {
	this.openDate.set(openDate);
    }

    /**
     * Returns the first three digits of the customer who owns this account.
     * 
     * @return the first three digits of the zip code.
     */
    public int getZipCode3()
    {
	return zipCode3.get();
    }

    /**
     * Sets the first three digits of the zip code of the customer who owns this
     * account.
     * 
     * @param zipCode
     *            The first three digits of the zip code.
     */
    public void setZipCode3(IntWritable zipCode)
    {
	this.zipCode3 = zipCode;
    }

    /**
     * Sets the first three digits of the zip code of the customer who owns this
     * account.
     * 
     * @param zipCode
     *            The first three digits of the zip code.
     */
    public void setZipCode3(int zipCode)
    {
	this.zipCode3 = new IntWritable(zipCode);
    }

    /**
     * @param ssn
     *            the ssn to set
     */
    public void setSsn(Text ssn)
    {
	this.ssn = ssn;
    }

    /**
     * @return the ssn
     */
    public Text getSsn()
    {
	return ssn;
    }

    /**
     * @param customerNumber
     *            the customerNumber to set
     */
    public void setCustomerNumber(Text customerNumber)
    {
	this.customerNumber = customerNumber;
    }

    /**
     * @return the customerNumber
     */
    public Text getCustomerNumber()
    {
	return customerNumber;
    }
    
    /**
     * @param statistics the statistics to set
     */
    public void setStatistics(AccountStatisticsWritable statistics)
    {
	this.statistics = statistics;
    }

    /**
     * @return the statistics
     */
    public AccountStatisticsWritable getStatistics()
    {
	return statistics;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
	ssn.readFields(in);
	customerNumber.readFields(in);
	accountNumber.readFields(in);
	openDate.readFields(in);
	zipCode3.readFields(in);
	
	if(in.readBoolean())
	{
	    statistics.readFields(in);
	}
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
	ssn.write(out);
	customerNumber.write(out);
	accountNumber.write(out);
	openDate.write(out);
	zipCode3.write(out);
	
	if(statistics != null)
	{
	    out.writeBoolean(true);
	    statistics.write(out);
	} 
	else 
	{
	    out.writeBoolean(false);
	}
    }

    @Override
    public int compareTo(AccountWritable a)
    {
	int cmp = openDate.compareTo(a.openDate);
	if (cmp != 0)
	{
	    return cmp;
	}
	cmp = accountNumber.compareTo(a.accountNumber);
	if (cmp != 0)
	{
	    return cmp;
	}
	return zipCode3.compareTo(a.zipCode3);
    }

    @Override
    public boolean equals(Object o)
    {
	if (o instanceof AccountWritable)
	{
	    AccountWritable a = (AccountWritable) o;
	    return openDate.equals(a.openDate)
					&& accountNumber
						.equals(a.accountNumber)
					&& zipCode3.equals(a.zipCode3);
	}
	return false;
    }

    @Override
    public int hashCode()
    {
	return openDate.hashCode() * 163 + accountNumber.hashCode()
				+ zipCode3.hashCode();
    }

    @Override
    public String toString()
    {
	StringBuilder builder = new StringBuilder();
	builder.append("," + openDate);
	builder.append("," + zipCode3);
	builder.append("," + ssn);
	builder.append("," + customerNumber);
	return builder.toString();
    }
}
