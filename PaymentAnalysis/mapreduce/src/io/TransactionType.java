package io;

public enum TransactionType
{
    CHARGE("Charge", 0),
    ADJUSTMENT("Adjustment", 1),
    PAYMENT("Payment", 2),
    UNKNOWN("", -1);
    
    private final String name;
    private final int offset;
    
    TransactionType(String name, int value) 
    {
	this.name = name;
	this.offset = value;
    }
    
    public String getName()
    {
	return this.name;
    }
    
    public int getOffset()
    {
	return this.offset;
    }
    
    public static TransactionType getByName(String name)
    {
	if(name == null)
	{
	    throw new IllegalArgumentException();
	}
	for(TransactionType t : TransactionType.values())
	{
	    if(t.name.equals(name)) 
	    {
		return t;
	    }
	}
	return UNKNOWN;
    }
    
    public static TransactionType getByValue(int value)
    {
	for(TransactionType t : TransactionType.values())
	    if(t.offset == value) 
		return t;
	return UNKNOWN;
    }

    /**
     * Number of valid transaction types (does not count UNKOWN)
     */
    public static final int NUM_TYPES = values().length - 1;  

}