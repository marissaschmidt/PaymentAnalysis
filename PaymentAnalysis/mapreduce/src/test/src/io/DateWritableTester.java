package test.src.io;
import io.DateWritable;
import io.InvalidDateException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;



public class DateWritableTester {
	public static void main(String[] args) {
		try {
		String testDate = new String("01/02/2010");
		String testDateAfter = new String("02/02/2010");
		String testDateBefore = new String("01/01/2009");
			DateWritable date;

				date = new DateWritable(testDate);

			DateWritable dateAfter = new DateWritable(testDateAfter);
			DateWritable dateBefore = new DateWritable(testDateBefore);

			if (date.compareTo(dateAfter) >= 0)
				System.out.println("compare after failed");
			if (date.compareTo(dateBefore) <= 0)
				System.out.println("compare before failed");
			if (date.compareTo(date) != 0)
				System.out.println("compare to equals failed");

			DateWritable newDate = new DateWritable();
			byte[] bytes;
			try {
				bytes = serialize(date);
				deserialize(newDate, bytes);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Date theDate = newDate.get();
			System.out.println(theDate);
			
			} catch (InvalidDateException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	}

	public static byte[] serialize(DateWritable date) throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);

		date.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}
	
	public static byte[] deserialize(DateWritable date, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		date.readFields(dataIn);
		dataIn.close();
		return bytes;
	}
}
