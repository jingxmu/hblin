package cn.edu.xmu.dm.hashjoin.lsh;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator{
	
	protected KeyComparator() {
		super(IntPair.class, true);
		// TODO Auto-generated constructor stub
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		IntPair ip1=(IntPair)a;
		IntPair ip2=(IntPair)b;
		return ip1.compareTo(ip2);
	}





}
