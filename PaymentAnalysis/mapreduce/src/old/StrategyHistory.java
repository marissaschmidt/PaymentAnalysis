package old;

import io.DateWritable;
import io.InvalidDateException;

import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * A WritableComparable for Account Strategy History events. 
 * 
 * @author Marissa Hollingsworth
 */
public class StrategyHistory extends EventWritable {
	
	public static final String GOOD_STANDING = "Good Standing";
	public static final String BAD_DEBT = "Bad Debt";
	
	public StrategyHistory(DateWritable date, Text type) {
		super(date, type);
	}
	
	public StrategyHistory(String date, String type) throws InvalidDateException {
		this(new DateWritable(date), new Text(type));
	}
	
	public StrategyHistory(Date date, String type) {
		this(new DateWritable(date), new Text(type));
	}
	
	public StrategyHistory() {
		super();
	}
}
