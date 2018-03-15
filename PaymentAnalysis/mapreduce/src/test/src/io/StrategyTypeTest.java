package test.src.io;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import io.EnumWritable;
import io.StrategyType;

public class StrategyTypeTest
{
    private static final String GOOD_STRATEGY = "Good Strategy";
    private static final String GOOD_STRATEGY_THIRTY = "Good Strategy 30";
    private static final String GOOD_STRATEGY_SIXTY = "Good Strategy 60";
    private static final String GOOD_STRATEGY_NINETY = "Good Strategy 90";
   
    private static final String BAD_DEBT = "Bad Debt";
    private static final String BAD_DEBT_THIRTY = "Bad Debt 30";
    private static final String BAD_DEBT_SIXTY = "Bad Debt 60";
    private static final String BAD_DEBT_NINETY = "Bad Debt 90";
   
    @Test
    public void testGetByName()
    {
	StrategyType good = StrategyType.getByName(GOOD_STRATEGY);
	assertEquals(good, StrategyType.GOOD_STRATEGY);
	
	StrategyType good30 = StrategyType.getByName(GOOD_STRATEGY_THIRTY);
	assertEquals(good30, StrategyType.GOOD_STRATEGY_THIRTY);
	
	StrategyType good60 = StrategyType.getByName(GOOD_STRATEGY_SIXTY);
	assertEquals(good60, StrategyType.GOOD_STRATEGY_SIXTY);
	
	StrategyType good90 = StrategyType.getByName(GOOD_STRATEGY_NINETY);
	assertEquals(good90, StrategyType.GOOD_STRATEGY_NINETY);

	StrategyType bad = StrategyType.getByName(BAD_DEBT);
	assertEquals(bad, StrategyType.BAD_DEBT);
	
	StrategyType bad30 = StrategyType.getByName(BAD_DEBT_THIRTY);
	assertEquals(bad30, StrategyType.BAD_DEBT_THIRTY);
	
	StrategyType bad60 = StrategyType.getByName(BAD_DEBT_SIXTY);
	assertEquals(bad60, StrategyType.BAD_DEBT_SIXTY);
	
	StrategyType bad90 = StrategyType.getByName(BAD_DEBT_NINETY);
	assertEquals(bad90, StrategyType.BAD_DEBT_NINETY);
	
	StrategyType invalid = StrategyType.getByName("");
	assertEquals(invalid, StrategyType.UNKNOWN);
    }
    
    @Test
    public void testGetByValue()
    {
	StrategyType good = StrategyType.getByValue(10);
	assertEquals(good, StrategyType.GOOD_STRATEGY);
	
	StrategyType good30 = StrategyType.getByValue(11);
	assertEquals(good30, StrategyType.GOOD_STRATEGY_THIRTY);
	
	StrategyType good60 = StrategyType.getByValue(12);
	assertEquals(good60, StrategyType.GOOD_STRATEGY_SIXTY);
	
	StrategyType good90 = StrategyType.getByValue(13);
	assertEquals(good90, StrategyType.GOOD_STRATEGY_NINETY);

	StrategyType bad = StrategyType.getByValue(14);
	assertEquals(bad, StrategyType.BAD_DEBT);
	
	StrategyType bad30 = StrategyType.getByValue(15);
	assertEquals(bad30, StrategyType.BAD_DEBT_THIRTY);
	
	StrategyType bad60 = StrategyType.getByValue(16);
	assertEquals(bad60, StrategyType.BAD_DEBT_SIXTY);
	
	StrategyType bad90 = StrategyType.getByValue(17);
	assertEquals(bad90, StrategyType.BAD_DEBT_NINETY);
	
	StrategyType invalid = StrategyType.getByValue(3);
	assertEquals(invalid, StrategyType.UNKNOWN);
    }
    
    @Test 
    public void testGetNextStrategy() 
    {
	StrategyType good = StrategyType.getNextStrategy(StrategyType.GOOD_STRATEGY);
	assertEquals(good, StrategyType.GOOD_STRATEGY_THIRTY);
	
	StrategyType good30 = StrategyType.getNextStrategy(StrategyType.GOOD_STRATEGY_THIRTY);
	assertEquals(good30, StrategyType.GOOD_STRATEGY_SIXTY);
	
	StrategyType good60 = StrategyType.getNextStrategy(StrategyType.GOOD_STRATEGY_SIXTY);
	assertEquals(good60, StrategyType.GOOD_STRATEGY_NINETY);
	
	StrategyType good90 = StrategyType.getNextStrategy(StrategyType.GOOD_STRATEGY_NINETY);
	assertEquals(good90, StrategyType.BAD_DEBT);

	StrategyType bad = StrategyType.getNextStrategy(StrategyType.BAD_DEBT);
	assertEquals(bad, StrategyType.BAD_DEBT_THIRTY);
	
	StrategyType bad30 = StrategyType.getNextStrategy(StrategyType.BAD_DEBT_THIRTY);
	assertEquals(bad30, StrategyType.BAD_DEBT_SIXTY);
	
	StrategyType bad60 = StrategyType.getNextStrategy(StrategyType.BAD_DEBT_SIXTY);
	assertEquals(bad60, StrategyType.BAD_DEBT_NINETY);
	
	StrategyType bad90 = StrategyType.getNextStrategy(StrategyType.BAD_DEBT_NINETY);
	assertEquals(bad90, StrategyType.UNKNOWN);
	
	StrategyType invalid = StrategyType.getNextStrategy(StrategyType.UNKNOWN);
	assertEquals(invalid, StrategyType.UNKNOWN);
    }
    
    
    @Test 
    public void testIO()
    {
	EnumWritable<StrategyType> type = new EnumWritable<StrategyType>(StrategyType.GOOD_STRATEGY);
	EnumWritable<StrategyType> deserialized = new EnumWritable<StrategyType>(StrategyType.UNKNOWN);
	
	try
	{
	    byte[] serialized = serialize(type);
	    deserialize(deserialized, serialized);
	} catch (IOException e)
	{
	    fail("Failed to write and read.");
	    e.printStackTrace();
	}
	
	try
	{
	    StrategyType after = deserialized.get(StrategyType.class);
	    assertEquals(StrategyType.GOOD_STRATEGY, after);
	    
	} catch (IOException e)
	{
	    fail("Failed to get strategy type.");
	    e.printStackTrace();
	}
    }
    
    public byte[] serialize(EnumWritable<StrategyType> type) throws IOException 
    {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream dataOut = new DataOutputStream(out);

	type.write(dataOut);
	dataOut.close();
	return out.toByteArray();
    }
	
    public byte[] deserialize(EnumWritable<StrategyType> type, byte[] bytes) throws IOException 
    {
	ByteArrayInputStream in = new ByteArrayInputStream(bytes);
	DataInputStream dataIn = new DataInputStream(in);
	type.readFields(dataIn);
	dataIn.close();
	return bytes;
    }
}
