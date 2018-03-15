package test.src.io;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import io.DateWritable;
import io.EnumWritable;
import io.TransactionType;

public class TransactionTypeTest
{
    private static final String CHARGE = "Charge";
    private static final String ADJUSTMENT = "Adjustment";
    private static final String PAYMENT = "Payment";
   
    @Test
    public void testGetByName()
    {
	TransactionType charge = TransactionType.getByName(CHARGE);
	assertEquals(charge, TransactionType.CHARGE);
	
	TransactionType adjustment = TransactionType.getByName(ADJUSTMENT);
	assertEquals(adjustment, TransactionType.ADJUSTMENT);
	
	TransactionType payment = TransactionType.getByName(PAYMENT);
	assertEquals(payment, TransactionType.PAYMENT);
	
	TransactionType invalid = TransactionType.getByName("");
	assertEquals(invalid, TransactionType.UNKNOWN);
    }
    
    @Test
    public void testGetByValue()
    {
	TransactionType charge = TransactionType.getByValue(0);
	assertEquals(charge, TransactionType.CHARGE);
	
	TransactionType adjustment = TransactionType.getByValue(1);
	assertEquals(adjustment, TransactionType.ADJUSTMENT);
	
	TransactionType payment = TransactionType.getByValue(2);
	assertEquals(payment, TransactionType.PAYMENT);
	
	TransactionType invalid = TransactionType.getByValue(3);
	assertEquals(invalid, TransactionType.UNKNOWN);
    }
    
    @Test 
    public void testIO()
    {
	EnumWritable<TransactionType> type = new EnumWritable<TransactionType>(TransactionType.CHARGE);
	EnumWritable<TransactionType> deserialized = new EnumWritable<TransactionType>(TransactionType.UNKNOWN);
	
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
	    TransactionType after = deserialized.get(TransactionType.class);
	    assertEquals(TransactionType.CHARGE, after);
	    
	} catch (IOException e)
	{
	    fail("Failed to get transaction type.");
	    e.printStackTrace();
	}
    }
    
    public byte[] serialize(EnumWritable<TransactionType> type) throws IOException 
    {
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	DataOutputStream dataOut = new DataOutputStream(out);

	type.write(dataOut);
	dataOut.close();
	return out.toByteArray();
    }
	
    public byte[] deserialize(EnumWritable<TransactionType> type, byte[] bytes) throws IOException 
    {
	ByteArrayInputStream in = new ByteArrayInputStream(bytes);
	DataInputStream dataIn = new DataInputStream(in);
	type.readFields(dataIn);
	dataIn.close();
	return bytes;
    }
}
