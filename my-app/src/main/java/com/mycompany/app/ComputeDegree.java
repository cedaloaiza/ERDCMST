package com.mycompany.app;

import com.google.common.collect.Iterables;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ComputeDegree extends
        BasicComputation<LongWritable, RDCMSTValue,
        FloatWritable, Text> {
    public void compute(Vertex<LongWritable, RDCMSTValue,
    		FloatWritable> vertex, Iterable<Text> iterable) throws IOException {
        if (getSuperstep() == 0){
            sendMessageToAllEdges(vertex, new Text());
        } else if (getSuperstep() == 1){
            Integer degree = Iterables.size(vertex.getEdges());
            //vertex.setValue(new LongWritable(degree));
        }else{
            vertex.voteToHalt();
        }
    }
}
