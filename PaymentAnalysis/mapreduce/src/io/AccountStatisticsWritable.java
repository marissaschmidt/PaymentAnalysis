package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class AccountStatisticsWritable implements Writable
{

    /** Sum of all Charge transactions during "Good Standing" strategy. */
    private DoubleWritable goodStandingCharges;
    /** Sum of all Charge transactions during "Bad Debt" strategy. */
    private DoubleWritable badDebtCharges;

    /** Sum of all Adjustment transactions during "Good Standing" strategy. */
    private DoubleWritable goodStandingAdjustments;
    /** Sum of all Adjustment transactions during "Bad Debt" strategy. */
    private DoubleWritable badDebtAdjustments;

    /**
     * Sum of all Payment transactions on account between 1 and 30 days after
     * "Good Standing" strategy start date.
     */
    private DoubleWritable goodStandingPayments30Day;
    /**
     * Sum of all Payment transactions on account between 1 and 60 days after
     * "Good Standing" strategy start date.
     */
    private DoubleWritable goodStandingPayments60Day;
    /**
     * Sum of all Payment transactions on account between 1 and 90 days after
     * "Good Standing" strategy start date.
     */
    private DoubleWritable goodStandingPayments90Day;
    /**
     * Sum of all Payment transactions on account after "Good Standing" strategy
     * start date but before "Bad Debt" strategy start date.
     */
    private DoubleWritable goodStandingPaymentsOther;

    /**
     * Sum of all Payment transactions on account between 1 and 30 days after
     * "Bad Debt" strategy start date.
     */
    private DoubleWritable badDebtPayments30Day;
    /**
     * Sum of all Payment transactions on account between 1 and 60 days after
     * "Bad Debt" strategy start date.
     */
    private DoubleWritable badDebtPayments60Day;
    /**
     * Sum of all Payment transactions on account between 1 and 90 days after
     * "Bad Debt" strategy start date.
     */
    private DoubleWritable badDebtPayments90Day;
    /**
     * Sum of all Payment transactions on account after "Bad Debt" strategy
     * start date.
     */
    private DoubleWritable badDebtPaymentsOther;

    /**
     * Number of Accounts with Open Date prior to this Account's Open Date which
     * have a Customer with the same SSN.
     */
    private IntWritable    previousAccountCount;

    /**
     * Sum of all Charge transactions occurring prior to this Account's Open
     * Date on Accounts which have a Customer with the same SSN (not same
     * account) which occurred while other Accounts were at or after
     * "Good Standing" Strategy start date but before other Accounts were at
     * "Bad Debt" Strategy start date.
     */
    private DoubleWritable previousAccountGoodStandingCharges;

    /**
     * Sum of all Adjustment transactions occurring prior to this Account's Open
     * Date on Accounts which have a Customer with the same SSN (not same
     * account) which occurred while other Accounts were at or after
     * "Good Standing" Strategy start date but before other Accounts were at
     * "Bad Debt" Strategy start date.
     */
    private DoubleWritable previousAccountGoodStandingAdjustments;

    /**
     * Sum of all Payment transactions occurring prior to this Account's Open
     * Date on Accounts which have a Customer with the same SSN (not same
     * account) which occurred while other Accounts were at or after
     * "Good Standing" Strategy start date but before other Accounts were at
     * "Bad Debt" Strategy start date.
     */
    private DoubleWritable previousAccountGoodStandingPayments;

    /**
     * Sum of all Payment transactions occurring prior to this Account's Open
     * Date on Accounts which have a Customer with the same SSN (not same
     * account) which occurred while other Accounts were at or after "Bad Debt"
     * Strategy start date.
     */
    private DoubleWritable previousAccountBadDebtPayments;
    
    public AccountStatisticsWritable()
    {
	goodStandingCharges = new DoubleWritable(0);
	badDebtCharges = new DoubleWritable(0);

	goodStandingAdjustments = new DoubleWritable(0);
	badDebtAdjustments = new DoubleWritable(0);

	goodStandingPayments30Day = new DoubleWritable(0);
	goodStandingPayments60Day = new DoubleWritable(0);
	goodStandingPayments90Day = new DoubleWritable(0);
	goodStandingPaymentsOther = new DoubleWritable(0);

	badDebtPayments30Day = new DoubleWritable(0);
	badDebtPayments60Day = new DoubleWritable(0);
	badDebtPayments90Day = new DoubleWritable(0);
	badDebtPaymentsOther = new DoubleWritable(0);

	previousAccountCount = new IntWritable(0);
	previousAccountGoodStandingCharges = new DoubleWritable(0);
	previousAccountGoodStandingAdjustments = new DoubleWritable(0);
	previousAccountGoodStandingPayments = new DoubleWritable(0);
	previousAccountBadDebtPayments = new DoubleWritable(0);
    }

    /**
     * Returns the "Good Standing" charges of this account.
     * 
     * @return the sum.
     */
    public double getGoodStandingCharges()
    {
	return goodStandingCharges.get();
    }

    /**
     * Sets the total of the charges that occurred during this Account's
     * "Good Standing" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setGoodStandingCharges(DoubleWritable amount)
    {
	goodStandingCharges = amount;
    }

    /**
     * Sets the total of the charges that occurred during this Account's
     * "Good Standing" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setGoodStandingCharges(double amount)
    {
	goodStandingCharges.set(amount);
    }

    /**
     * Returns the "Bad Debt" charges of this account.
     * 
     * @return the sum.
     */
    public double getBadDebtCharges()
    {
	return badDebtCharges.get();
    }

    /**
     * Sets the total of the charges that occurred during this Account's
     * "Bad Debt" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setBadDebtCharges(DoubleWritable amount)
    {
	badDebtCharges = amount;
    }

    /**
     * Sets the total of the charges that occurred during this Account's
     * "Bad Debt" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setBadDebtCharges(double amount)
    {
	badDebtCharges.set(amount);
    }

    /**
     * Returns the "Good Standing" adjustments of this account.
     * 
     * @return the sum.
     */
    public double getGoodStandingAdjustments()
    {
	return goodStandingAdjustments.get();
    }

    /**
     * Sets the total of the adjustments that occurred during this Account's
     * "Good Standing" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setGoodStandingAdjustments(DoubleWritable amount)
    {
	goodStandingAdjustments = amount;
    }

    /**
     * Sets the total of the adjustments that occurred during this Account's
     * "Good Standing" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setGoodStandingAdjustments(double amount)
    {
	goodStandingAdjustments.set(amount);
    }

    /**
     * Returns the total of the adjustments that occurred during this Account's
     * "Bad Debt" strategy.
     * 
     * @return the sum.
     */
    public double getBadDebtAdjustments()
    {
	return badDebtAdjustments.get();
    }

    /**
     * Sets the total of the adjustments that occurred during this Account's
     * "Bad Debt" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setBadDebtAdjustments(DoubleWritable amount)
    {
	badDebtAdjustments = amount;
    }

    /**
     * Sets the total of the adjustments that occurred during this Account's
     * "Bad Debt" strategy.
     * 
     * @param amount
     *            The sum.
     */
    public void setBadDebtAdjustments(double amount)
    {
	badDebtAdjustments.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during the first 30 days
     * of this Account's "Good Standing" strategy.
     * 
     * @return the sum.
     */
    public double getGoodStandingPayments30Day()
    {
	return goodStandingPayments30Day.get();
    }

    /**
     * Sets the total of the payments that occurred during the first 30 days of
     * this Account's "Good Standing" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setGoodStandingPayments30Day(double amount)
    {
	goodStandingPayments30Day.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during the first 60 days
     * of this Account's "Good Standing" strategy.
     * 
     * @return the sum.
     */
    public double getGoodStandingPayments60Day()
    {
	return goodStandingPayments60Day.get();
    }

    /**
     * Sets the total of the payments that occurred during the first 60 days of
     * this Account's "Good Standing" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setGoodStandingPayments60Day(double amount)
    {
	goodStandingPayments60Day.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during the first 90 days
     * of this Account's "Good Standing" strategy.
     * 
     * @return the sum.
     */
    public double getGoodStandingPayments90Day()
    {
	return goodStandingPayments90Day.get();
    }

    /**
     * Sets the total of the payments that occurred during the first 90 days of
     * this Account's "Good Standing" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setGoodStandingPayments90Day(double amount)
    {
	goodStandingPayments90Day.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during this Account's
     * "Good Standing" strategy.
     * 
     * @return the sum.
     */
    public double getGoodStandingPaymentsOther()
    {
	return goodStandingPaymentsOther.get();
    }

    /**
     * Sets the total of the payments that occurred during this Account's
     * "Good Standing" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setGoodStandingPaymentsOther(double amount)
    {
	goodStandingPaymentsOther.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during the first 30 days
     * of this Account's "Bad Debt" strategy.
     * 
     * @return the total amount.
     */
    public double getBadDebtPayments30Day()
    {
	return badDebtPayments30Day.get();
    }

    /**
     * Sets the total of the payments that occurred during the first 30 days of
     * this Account's "Bad Debt" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setBadDebtPayments30Day(double amount)
    {
	badDebtPayments30Day.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during the first 60 days
     * of this Account's "Bad Debt" strategy.
     * 
     * @return the total amount.
     */
    public double getBadDebtPayments60Day()
    {
	return badDebtPayments60Day.get();
    }

    /**
     * Sets the total of the payments that occurred during the first 60 days of
     * this Account's "Bad Debt" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setBadDebtPayments60Day(double amount)
    {
	badDebtPayments60Day.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during the first 90 days
     * of this Account's "Bad Debt" strategy.
     * 
     * @return the total amount.
     */
    public double getBadDebtPayments90Day()
    {
	return badDebtPayments90Day.get();
    }

    /**
     * Sets the total of the payments that occurred during the first 90 days of
     * this Account's "Bad Debt" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setBadDebtPayments90Day(double amount)
    {
	badDebtPayments90Day.set(amount);
    }

    /**
     * Returns the total of the payments that occurred during this Account's
     * "Bad Debt" strategy.
     * 
     * @return the total amount.
     */
    public double getBadDebtPaymentsOther()
    {
	return badDebtPaymentsOther.get();
    }

    /**
     * Sets the total of the payments that occurred during this Account's
     * "Bad Debt" strategy.
     * 
     * @param amount
     *            The total amount.
     */
    public void setBadDebtPaymentsOther(double amount)
    {
	badDebtPaymentsOther.set(amount);
    }

    /**
     * Returns the sum of all Charge transactions that occurred during the life
     * of this Account.
     * 
     * @return The "Good Standing" charges plus the "Bad Debt" charges.
     */
    public double getTotalCharges()
    {
	return goodStandingCharges.get() + badDebtCharges.get();
    }

    /**
     * Returns the sum of all Adjustment transactions that occurred during the
     * life of this Account.
     * 
     * @return The "Good Standing" adjustments plus the "Bad Debt" adjustments.
     */
    public double getTotalAdjustments()
    {
	return goodStandingAdjustments.get() + badDebtAdjustments.get();
    }

    /**
     * Returns the adjusted total charges of this Account.
     * 
     * @return The total charges minus the total adjustments.
     */
    public double getAdjustedTotalCharges()
    {
	return getTotalCharges() + getTotalAdjustments();
    }

    /**
     * Returns the sum all Payment transactions that occurred during this
     * Account's "Good Standing" strategy.
     * 
     * @return The total amount.
     */
    public double getGoodStandingPayments()
    {
	return goodStandingPaymentsOther.get();
    }

    /**
     * Returns the sum all Payment transactions that occurred during this
     * Account's "Bad Debt" strategy.
     * 
     * @return The total amount.
     */
    public double getBadDebtPayments()
    {
	return badDebtPaymentsOther.get();
    }

    /**
     * Returns the "Bad Debt" transfer balance of this Account.
     * 
     * @return The sum of all "Good Standing" charges, adjustments and payments.
     */

    public double getBadDebtTransferBalance()
    {
	return goodStandingCharges.get() + goodStandingAdjustments.get() + getGoodStandingPayments();
    }

    public IntWritable getPreviousAccountCount()
    {
	return previousAccountCount;
    }

    public void setPreviousAccountCount(IntWritable count)
    {
	this.previousAccountCount = count;
    }

    public void setPreviousAccountCount(int count)
    {
	this.previousAccountCount = new IntWritable(count);
    }

    public DoubleWritable getPreviousAccountGoodStandingCharges()
    {
	return previousAccountGoodStandingCharges;
    }

    public void setPreviousAccountGoodStandingCharges(DoubleWritable amount)
    {
	this.previousAccountGoodStandingCharges = amount;
    }

    public void setPreviousAccountGoodStandingCharges(double amount)
    {
	this.previousAccountGoodStandingCharges = new DoubleWritable(amount);
    }

    public DoubleWritable getPreviousAccountGoodStandingAdjustments()
    {
	return previousAccountGoodStandingAdjustments;
    }

    public void setPreviousAccountGoodStandingAdjustments(DoubleWritable amount)
    {
	this.previousAccountGoodStandingAdjustments = amount;
    }

    public void setPreviousAccountGoodStandingAdjustments(double amount)
    {
	this.previousAccountGoodStandingAdjustments = new DoubleWritable(amount);
    }

    public DoubleWritable getPreviousAccountGoodStandingPayments()
    {
	return previousAccountGoodStandingPayments;
    }

    public void setPreviousAccountGoodStandingPayments(DoubleWritable amount)
    {
	this.previousAccountGoodStandingPayments = amount;
    }

    public void setPreviousAccountGoodStandingPayments(double amount)
    {
	this.previousAccountGoodStandingPayments = new DoubleWritable(amount);
    }

    public DoubleWritable getPreviousAccountBadDebtPayments()
    {
	return previousAccountBadDebtPayments;
    }

    public void setPreviousAccountBadDebtPayments(DoubleWritable amount)
    {
	this.previousAccountBadDebtPayments = amount;
    }

    public void setPreviousAccountBadDebtPayments(double amount)
    {
	this.previousAccountBadDebtPayments = new DoubleWritable(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
	goodStandingCharges.readFields(in);
	badDebtCharges.readFields(in);

	goodStandingAdjustments.readFields(in);
	badDebtAdjustments.readFields(in);

	goodStandingPayments30Day.readFields(in);
	goodStandingPayments60Day.readFields(in);
	goodStandingPayments90Day.readFields(in);
	goodStandingPaymentsOther.readFields(in);

	badDebtPayments30Day.readFields(in);
	badDebtPayments60Day.readFields(in);
	badDebtPayments90Day.readFields(in);
	badDebtPaymentsOther.readFields(in);

	previousAccountCount.readFields(in);
	previousAccountGoodStandingCharges.readFields(in);
	previousAccountGoodStandingAdjustments.readFields(in);
	previousAccountGoodStandingPayments.readFields(in);
	previousAccountBadDebtPayments.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
	goodStandingCharges.write(out);
	badDebtCharges.write(out);

	goodStandingAdjustments.write(out);
	badDebtAdjustments.write(out);

	goodStandingPayments30Day.write(out);
	goodStandingPayments60Day.write(out);
	goodStandingPayments90Day.write(out);
	goodStandingPaymentsOther.write(out);

	badDebtPayments30Day.write(out);
	badDebtPayments60Day.write(out);
	badDebtPayments90Day.write(out);
	badDebtPaymentsOther.write(out);

	previousAccountCount.write(out);
	previousAccountGoodStandingCharges.write(out);
	previousAccountGoodStandingAdjustments.write(out);
	previousAccountGoodStandingPayments.write(out);
	previousAccountBadDebtPayments.write(out);
    }

    @Override
    public int hashCode()
    {
	return goodStandingCharges.hashCode() * 163 +
				+ badDebtCharges.hashCode()
				+ goodStandingAdjustments.hashCode()
				+ badDebtAdjustments.hashCode();
    }

    @Override
    public String toString()
    {
	DecimalFormat df = new DecimalFormat("#.##");
	StringBuilder builder = new StringBuilder();
	builder.append("," + df.format(getTotalCharges()));
	builder.append("," + df.format(getTotalAdjustments()));
	builder.append("," + df.format(getAdjustedTotalCharges()));
	builder.append("," + df.format(goodStandingPayments30Day.get()));
	builder.append("," + df.format(goodStandingPayments60Day.get()));
	builder.append("," + df.format(goodStandingPayments90Day.get()));
	builder.append("," + df.format(getBadDebtTransferBalance()));
	builder.append("," + df.format(badDebtPayments30Day.get()));
	builder.append("," + df.format(badDebtPayments60Day.get()));
	builder.append("," + df.format(badDebtPayments90Day.get()));
	builder.append("," + df.format(previousAccountCount.get()));
	builder.append("," + df.format(previousAccountGoodStandingCharges.get()));
	builder.append("," + df.format(previousAccountGoodStandingAdjustments.get()));
	builder.append("," + df.format(previousAccountGoodStandingPayments.get()));
	builder.append("," + df.format(previousAccountBadDebtPayments.get()));
	return builder.toString();
    }

}
