package old;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class AccountEvent implements Writable {

	private Object[] classes = {Transaction.class, StrategyHistory.class};
	
	private Class<? extends EventWritable> eventClass;
	private int index;
	private EventWritable event;
	
	public AccountEvent() {
		this(EventWritable.class, new EventWritable());
	}
	
	public AccountEvent(Class<? extends EventWritable> eventClass) {
		this(eventClass, new EventWritable());
	}

	public AccountEvent(Class<? extends EventWritable> eventClass,
			EventWritable event) {
		if (eventClass == null) {
			throw new IllegalArgumentException("null eventClass");
		}
		this.eventClass = eventClass;
		this.event = event;
	}

	public Class<? extends EventWritable> getEventClass() {
		return eventClass;
	}

	public void set(EventWritable event) {
		this.event = event;
	}

	public EventWritable get() {
		return event;
	}

	public void readFields(DataInput in) throws IOException { 
		Writable value = WritableFactories.newInstance(eventClass);
		value.readFields(in);
		event = (EventWritable) value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		event.write(out);
	}
	
	public String toString() {
		return event.toString();
	}
}
