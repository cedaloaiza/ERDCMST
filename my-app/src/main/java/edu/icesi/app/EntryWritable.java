package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.Writable;

public class EntryWritable implements Writable {
	
	private Writable key;
	private Writable value;
	
	public EntryWritable() {

	}

	public EntryWritable(Writable key, Writable value) {
		super();
		this.key = key;
		this.value = value;
	}
	
	public Writable get(Writable key) {
		return value;
	}
	
	public Writable getKey() {
		return key;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
		
	}
	
	

}
