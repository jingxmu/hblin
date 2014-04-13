package cn.edu.xmu.dm.hashjoin.lsh;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SegmentPartitioner extends Partitioner<IntPair,IntWritable>{

	@Override
	public int getPartition(IntPair key, IntWritable value, int reduceNum) {
		// TODO Auto-generated method stub
		return Math.abs(key.getSegment())%reduceNum;
	}

}
