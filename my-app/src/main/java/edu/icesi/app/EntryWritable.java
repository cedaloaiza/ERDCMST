package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EntryWritable implements Writable {
	
	private GenericWritable key;
	private GenericWritable value;
	
	public EntryWritable() {
		this.key = new GenericWritable() {
			
			@Override
			protected Class<? extends Writable>[] getTypes() {
				Class[] CLASSES = {
			               IntWritable.class, 
			               Text.class,
			               };
				return CLASSES;
			}
		};
		this.value = new GenericWritable() {
			
			@Override
			protected Class<? extends Writable>[] getTypes() {
				Class[] CLASSES = {
			               PositionWritable.class, 
			               DoubleWritable.class,
			               };
				return CLASSES;
			}
		};
	}

	public EntryWritable(Writable key, Writable value) {
		//super();
		this.key = new GenericWritable() {
			
			@Override
			protected Class<? extends Writable>[] getTypes() {
				Class[] CLASSES = {
			               IntWritable.class, 
			               Text.class,
			               };
				return CLASSES;
			}
		};
		this.value = new GenericWritable() {
					
					@Override
					protected Class<? extends Writable>[] getTypes() {
						Class[] CLASSES = {
					               PositionWritable.class, 
					               DoubleWritable.class,
					               };
						return CLASSES;
					}
		};
		this.key.set(key);
		this.value.set(value);
	}
	
	public Writable get(Writable key) {
		return value.get();
	}
	
	public Writable getKey() {
		return key.get();
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
