package aggregators;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;

import edu.icesi.app.EntryWritable;

public class BsCombiner implements MessageCombiner<IntWritable, EntryWritable> {


	public void combine(IntWritable vertexIndex, EntryWritable originalMessage, EntryWritable messageToCombine) {
		Text messageKeyOriginal = (Text) originalMessage.getKey();
		Text messageKeyComing = (Text) messageToCombine.getKey();
		if ( messageKeyOriginal.toString().equals("POSSB") && messageKeyComing.toString().equals("POSSB")) {
			
		};
	}

	public EntryWritable createInitialMessage() {
		// TODO Auto-generated method stub
		return null;
	}

}
