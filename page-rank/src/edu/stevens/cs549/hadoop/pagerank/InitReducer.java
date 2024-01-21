package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */
		int defualtrank = 1;
		String adjList = "";
		for (Text value : values) {
			adjList += value.toString() + " ";
		}
		context.write(new Text(key + ";PD;" + defualtrank), new Text(adjList.substring(0,adjList.length()-1)));
	}
}
