package edu.icesi.app;


import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>double</code> vertex values and <code>float</code>
  * out-edge weights, and <code>double</code> message types,
  *  specified in JSON format.
  */
public class ISRDCMSTVertexInputFormat extends
  TextVertexInputFormat<IntWritable, RDCMSTValue, DoubleWritable> {
	
  private static final Logger LOG =
		   Logger.getLogger(ISRDCMSTVertexInputFormat.class);

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new RDCMSTVertexReader();
  }

 /**
  * VertexReader that features <code>double</code> vertex
  * values and <code>float</code> out-edge weights. The
  * files should be in the following JSON format:
  * JSONArray(<vertex id>, <vertex value>,
  *   JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
  * Here is an example with vertex id 1, vertex value 4.3, and two edges.
  * First edge has a destination vertex 2, edge value 2.1.
  * Second edge has a destination vertex 3, edge value 0.7.
  * [1,4.3,[[2,2.1],[3,0.7]]]
  */
  class  RDCMSTVertexReader extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected IntWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new IntWritable(jsonVertex.getInt(0));
    }

    @Override
    protected RDCMSTValue getValue(JSONArray jsonVertex) throws
      	JSONException, IOException {
      
      int parent = jsonVertex.getInt(5);
      double f = 0;
      double b = 0;
      JSONArray jsonDistances = jsonVertex.getJSONArray(1);
      JSONArray jsonPositions = jsonVertex.getJSONArray(6);
      double[] distances = new double[jsonDistances.length()];
      Position[] positions = new Position[jsonDistances.length()];
      if (LOG.isDebugEnabled()) {
    	  System.out.println("Reading node:: " + jsonVertex.getInt(0));
          System.out.println("distances array:: " + jsonDistances.length());
      }
      
      for(int i = 0; i < jsonDistances.length(); i++){
    	  distances[i] = jsonDistances.getDouble(i);
    	  Position pos = Position.NONE;
    	  if (jsonPositions.getString(i).equals("p")) {
    		  pos = Position.PREDECESSOR;
    	  } else if (jsonPositions.getString(i).equals("s")) {
    		  pos = Position.SUCCESSOR;
    	  }
    	  positions[i] = pos;
      }
      b = jsonVertex.getDouble(3);
      f = jsonVertex.getDouble(4);
      return new RDCMSTValue(f, b, distances, positions, parent, true, jsonVertex.getInt(0));
    }

    @Override
    protected Iterable<Edge<IntWritable, DoubleWritable>> getEdges(
        JSONArray jsonVertex) throws JSONException, IOException {
      List<Edge<IntWritable, DoubleWritable>> edges = Lists.newArrayList();
      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
      edges = Lists.newArrayListWithCapacity(jsonEdgeArray.length());
      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        int successor = jsonEdgeArray.getInt(i);
        edges.add(EdgeFactory.create(new IntWritable(successor),
            new DoubleWritable( jsonVertex.getJSONArray(1).getDouble(successor))));
      }
     
      return edges;
    }

    @Override
    protected Vertex<IntWritable, RDCMSTValue, DoubleWritable> handleException(Text line, JSONArray jsonVertex, JSONException e) {
      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }

  }
}