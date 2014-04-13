package cn.edu.xmu.dm.hashjoin.lsh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {

	private int segment = 0;
	private int bucketId = 0;

	public IntPair(){
		segment=0;
		bucketId=0;
	}
	public IntPair(int segment,int bucketId){
		this.segment=segment;
		this.bucketId=bucketId;
	}
	
	public int getSegment() {
		return segment;
	}

	public void setSegment(int segment) {
		this.segment = segment;
	}

	public int getBucketId() {
		return bucketId;
	}

	public void setBucketId(int bucketId) {
		this.bucketId = bucketId;
	}

	public String toString() {
		return segment + "\t" + bucketId;
	}

	/**
	 * Read the two integers. Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE,
	 * MAX_VALUE-> -1
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		segment = in.readInt();
		bucketId = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(segment);
		out.writeInt(bucketId);
	}

	@Override
	public int hashCode() {
		return segment * 157 + bucketId;
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof IntPair) {
			IntPair r = (IntPair) right;
			return r.segment == segment && r.bucketId == bucketId;
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(IntPair right) {
		if (right.segment != segment) {
			return segment > right.segment ? 1 : -1;
		} else if (right.bucketId != bucketId) {
			return bucketId > right.bucketId ? 1 : -1;
		}
		return 0;
	}

}
