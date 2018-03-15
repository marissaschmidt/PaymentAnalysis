package gen;

public enum HistogramType 
{
    GOOD_CHARGE("GoodStandingCharges"),
    BAD_CHARGE("BadDebtCharges"),
    GOOD_ADJUSTMENT("GoodStandingAdjustments"),
    BAD_ADJUSTMENT("BadDebtAdjustments"),
    GOOD_PAYMENT("GoodStandingPayments"),
    BAD_PAYMENT("BadDebtPayments"),
    CUSTOMERS_PER_ACCOUNT("CustomersPerAccount"),
    DEFAULT("default");
	
    private final String name;

    HistogramType(String name) 
    {
	this.name = name;
    }

    public static HistogramType getType(int val)
    {
	for(HistogramType t : values())
	{
	    if(t.ordinal() == val)
	    {
		return t;
	    }
	}
	return null;
    }
	
    public String getName() 
    {
	return name;
    }
}

