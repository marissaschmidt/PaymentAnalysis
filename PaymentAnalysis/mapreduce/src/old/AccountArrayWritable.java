package old;

import io.AccountWritable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * A WritableComparable for an array of Account objects. 
 * 
 * @author Marissa Hollingsworth
 */
public class AccountArrayWritable extends ArrayWritable 
{
		public AccountArrayWritable() {
			super(AccountWritable.class, new AccountWritable[0]);
		}
		
		public AccountArrayWritable(AccountWritable[] accounts) {
			super(AccountWritable.class, accounts);
		}

		public String toString() {
			StringBuilder builder = new StringBuilder();
			Writable[] values = this.get();
			for(int i = 0; i < values.length; i++) {
				builder.append("[");
				builder.append(values[i]);
				builder.append("]");
			}
			return builder.toString();
		}
}
