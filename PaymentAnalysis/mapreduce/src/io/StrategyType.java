package io;

public enum StrategyType
{
    GOOD_STRATEGY("Good Strategy", 0, 0),
    GOOD_STRATEGY_THIRTY("Good Strategy 30", 30, 1),
    GOOD_STRATEGY_SIXTY("Good Strategy 60", 60, 2),
    GOOD_STRATEGY_NINETY("Good Strategy 90", 90, 3),
   
    BAD_DEBT("Bad Debt", 0, 4), 
    BAD_DEBT_THIRTY("Bad Debt 30", 30, 5),
    BAD_DEBT_SIXTY("Bad Debt 60", 60, 6),
    BAD_DEBT_NINETY("Bad Debt 90", 90, 7),
    
    UNKNOWN("", -1, -1);
    
    private final String name;
    private final int value;
    private final int offset;
    
    StrategyType(String name, int value, int offset) 
    {
	this.name = name;
	this.value = value;
	this.offset = offset;
    }
    
    public String getName() 
    {
	return this.name;
    }
    public int getNumberOfDays()
    {
	return this.value;
    }
    public int getOffset()
    {
	return this.offset;
    }
    
    public boolean isGoodStrategy()
    {
	if(name.startsWith("Good Strategy"))
	{
	    return true;
	}
	return false;
    }
    
    public static final int STRATEGY_OFFSET = 10;
    public static final int MAX_STRATEGY_VALUE = 90;

    public static StrategyType getByName(String name)
    {
	if(name == null)
	{
	    throw new IllegalArgumentException();
	}
	for(StrategyType t : StrategyType.values())
	{
	    if(t.name.equals(name)) 
	    {
		return t;
	    }
	}
	return UNKNOWN;
    }
    
    public static StrategyType getByValue(int value)
    {
	for(StrategyType t : StrategyType.values())
	{
	    if(t.value == value) 
	    {
		return t;
	    }
	}
	return UNKNOWN;
    }
    
    
    public static StrategyType getNextStrategy(StrategyType type)
    {
	//if(type.value == 17) return GOOD_STRATEGY;
	return getByValue(type.value + 1);
    }
    
    /**
     * Number of valid strategy types (does not count UNKOWN)
     */
    public static final int NUM_TYPES = values().length - 1;  
}
