package com.mycompany.app;

import com.google.common.collect.Iterables;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ComputeDegree extends
        BasicComputation<IntWritable, IntWritable,
                NullWritable, Text> {
    public void compute(Vertex<IntWritable, IntWritable,
            NullWritable> vertex, Iterable<Text> iterable) throws IOException {
        if (getSuperstep() == 0){
            sendMessageToAllEdges(vertex, new Text());
        } else if (getSuperstep() == 1){
            Integer degree = Iterables.size(vertex.getEdges());
            vertex.setValue(new IntWritable(degree));
        }else{
            vertex.voteToHalt();
        }
    }
}
