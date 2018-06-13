package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PositionWritable implements Writable {
	
	private Position pos;
	
	public PositionWritable() {
    }

    public PositionWritable(Position p) {
        this.pos = p;
    }

    public Position getPosition() {
        return pos;
    }

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeEnum(out, pos);
	}

	@Override
	public void readFields(DataInput in) throws IOException {	
		 pos = WritableUtils.readEnum(in, Position.class);
		
	}
}
