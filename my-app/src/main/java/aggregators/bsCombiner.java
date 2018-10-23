package aggregators;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.icesi.app.EntryWritable;

public class bsCombiner implements MessageCombiner<IntWritable, EntryWritable> {


	public void combine(IntWritable vertexIndex, EntryWritable originalMessage, EntryWritable messageToCombine) {
				
	}

	public EntryWritable createInitialMessage() {
		// TODO Auto-generated method stub
		return null;
	}

}
