package cn.edu.xmu.dm.hashjoin.lsh;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SegmentGroup extends WritableComparator {

	protected SegmentGroup() {
		super(IntPair.class, true);
	}

	@SuppressWarnings("rawtypes")	// /???
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair ip1 = (IntPair) a;
		IntPair ip2 = (IntPair) b;
		int seg1 = ip1.getSegment();
		int seg2 = ip2.getSegment();
		return seg1 > seg2 ? 1 : seg1 < seg2 ? -1 : 0;
	}

}
