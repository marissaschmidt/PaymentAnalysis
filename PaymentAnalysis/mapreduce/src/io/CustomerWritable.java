package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import old.AccountArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * A WritableComparable for Customer objects. 
 * 
 * @author Marissa Hollingsworth
 */
public class CustomerWritable implements WritableComparable<CustomerWritable> 
{	
	/** Unique Customer identifier. */
	private Text customerNumber;
	/**	First three digits of Customer's zip code. */
	private IntWritable zipCode3;
	/**	The Accounts owned by this Customer. */
	private AccountArrayWritable accounts;

	/**
	 * Creates a new <code>Customer</code> instance.
	 */
	public CustomerWritable() {
		this(new Text(),  new IntWritable(0), new AccountArrayWritable());
	}
	
	/**
	 * Creates a new <code>Customer</code> instance with the specified values. 
	 * @param customerNumber The identifier for this Customer.
	 * @param zipCode3	The first three digits of this Customer's zip code.
	 * @param accounts The Accounts owned by this customer.
	 */
	public CustomerWritable(Text customerNumber, IntWritable zipCode3, AccountArrayWritable accounts) {
		this.setCustomerNumber(customerNumber);
		this.setZipCode3(zipCode3);
		this.setAccounts(accounts);
	}
	
	/**
	 * Creates a new <code>Customer</code> instance with the specified values. 
	 * @param customerNumber The identifier for this Customer.
	 * @param zipCode3	The first three digits of this Customer's zip code.
	 */
	public CustomerWritable(String customerNumber, String zipCode3) {
		this(new Text(customerNumber), new IntWritable(Integer.parseInt(zipCode3)), new AccountArrayWritable());
	}
	
	/**
	 * Creates a new <code>Customer</code> instance with the customer number . 
	 * @param customerNumber The identifier for this Customer.
	 */
	public CustomerWritable(Text customerNumber) {
		this(new Text(customerNumber), new IntWritable(0), new AccountArrayWritable());
	}
	
	/**
	 * Creates a new <code>Customer</code> instance with the specified past accounts. 
	 * @param accounts An array of past Accounts owned by this Customer.
	 */
	public CustomerWritable(AccountWritable[] accounts) {
		this(new Text(),  new IntWritable(0), new AccountArrayWritable(accounts));
	}
	
	/**
	 * Returns the unique customer identifier for this Customer.
	 * @return The customer number.
	 */
	public String getCustomerNumber() {
		return customerNumber.toString();
	}
	
	/**
	 * Sets the unique customer identifier for this Customer.
	 * @param customerNumber The customer number.
	 */
	public void setCustomerNumber(Text customerNumber) {
		this.customerNumber = customerNumber;
	}
	
	/**
	 * Sets the unique customer identifier for this Customer.
	 * @param customerNumber The customer number.
	 */
	public void setCustomerNumber(String customerNumber) {
		this.customerNumber.set(customerNumber);
	}
	
	/**
	 * Returns the first three digits of this Customer.
	 * @return the first three digits of the zip code. 
	 */
	public int getZipCode3() {
		return zipCode3.get();
	}
	
	/**
	 * Sets the first three digits of the zip code of this Customer. 
	 * @param zipCode The first three digits of the zip code.
	 */
	public void setZipCode3(IntWritable zipCode3) {
		this.zipCode3 = zipCode3;
	}
	
	/**
	 * Sets the first three digits of the zip code of this Customer. 
	 * @param zipCode The first three digits of the zip code.
	 */
	public void setZipCode3(String zipCode3) {
		this.zipCode3 = new IntWritable(Integer.parseInt(zipCode3));
	}
	
	/**
	 * Sets the first three digits of the zip code of this Customer. 
	 * @param zipCode The first three digits of the zip code.
	 */
	public void setZipCode3(int zipCode3) {
		this.zipCode3 = new IntWritable(zipCode3);
	}
	
	/**
	 * Returns the Accounts owned by this Customer.
	 * @return an array of accounts.
	 */
	public AccountWritable[] getAccounts() {
		AccountWritable[] accountArray = new AccountWritable[accounts.get().length];
		for(int i = 0; i < accountArray.length; i++)
			accountArray[i] = (AccountWritable) accounts.get()[i];
		return accountArray;
	}
	
	/**
	 * Sets the accounts owned by this Customer.
	 * @param accounts An array of accounts.
	 */
	public void setAccounts(AccountArrayWritable accounts){
		this.accounts = accounts;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		customerNumber.readFields(in);
		zipCode3.readFields(in);
		accounts.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		customerNumber.write(out);
		zipCode3.write(out);
		accounts.write(out);
	}

	@Override
	public int compareTo(CustomerWritable c) {
		return customerNumber.compareTo(c.customerNumber);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof CustomerWritable) {
			CustomerWritable c = (CustomerWritable) o;
			return customerNumber.equals(c.customerNumber);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return customerNumber.hashCode() * 163 + zipCode3.hashCode() + accounts.hashCode();
	}

	@Override
	public String toString() {
		return "customerNumber[" + customerNumber + "] zipCode3[" + zipCode3 + "] accounts[" + accounts + "]" ;
	}
}
