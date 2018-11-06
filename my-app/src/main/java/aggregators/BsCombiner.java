package aggregators;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import edu.icesi.app.BFsUpdateAndBestLocationBeginningComputation;

public class BsCombiner implements MessageCombiner<IntWritable, DoubleWritable> {

	private static final Logger LOG =
		      Logger.getLogger(BFsUpdateAndBestLocationBeginningComputation.class);

	public void combine(IntWritable vertexIndex, DoubleWritable originalMessage, DoubleWritable messageToCombine) {
		if (LOG.isDebugEnabled()) {
			System.out.println("Combining " + originalMessage + " and " + messageToCombine);
		}
		if (messageToCombine.get() > originalMessage.get()) {
			originalMessage.set(messageToCombine.get());
		}
	}

	public DoubleWritable createInitialMessage() {
		// TODO Auto-generated method stub
		return new DoubleWritable(0);
	}

}
