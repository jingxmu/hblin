package cn.edu.xmu.dm.hashjoin.lsh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map:对每条record的最小哈希签名分段进行映射，如果不同的record的相同段映射到相同的hash值，则将它们作为一个候选对 
 * input: key:offset value:签名 
 * output: key:IntPair对， value:recordId 
 * 利用到了自定义的IntPair,二次排序
 * 相同的段，会被分到同一组，相同的段，相同的bucketId会被分到同一个桶中，同一个桶中的recordId对会被选为候选对
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */

public class LSHPhase1Mapper extends
		Mapper<LongWritable, Text, IntPair, IntWritable> {

	private IntPair intPair = new IntPair();
	private IntWritable recordId = new IntWritable(0);
	private Configuration conf;
	private int hashNum;
	private int columnsNum;
	private int segmentNum;
	private int bucketsLength;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		hashNum = conf.getInt("hashNum", hashNum);
		columnsNum = conf.getInt("columnsNum", columnsNum);
		bucketsLength = conf.getInt("bucketsLength", 0);
		if (bucketsLength == 0) {
			throw new RuntimeException("buckets length is error.");
		}
		segmentNum = hashNum / columnsNum;

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] recordIdAndTokens = value.toString().split("\t");
		String[] sig = recordIdAndTokens[1].split(conf.get("WORD_SEPARATOR"));
		int[] signature = new int[sig.length];
		int[] seg = new int[columnsNum];
		int count = 0;
		int bucketId = 0;
		for (int i = 0; i < sig.length; i++) {
			signature[i] = Integer.parseInt(sig[i]);
		}
		for (int segId = 0; segId < segmentNum; segId++) {
			count = 0;
			while (count < columnsNum) {
				seg[count] = signature[segId * columnsNum + count];
				count++;
			}
			bucketId = getHash(seg, segId);
			intPair.setSegment(segId);
			intPair.setBucketId(bucketId);
			recordId.set(Integer.parseInt(recordIdAndTokens[0]));
			context.write(intPair, recordId);
		}
	}

	public int getHash(int[] seg, int key) {
		int hashValue;
		int sum1 = 0, sum2 = 0;
		for (int i = 0; i < seg.length; i++) {
			if (i % 2 == 0) {
				sum1 += seg[i];
			} else {
				sum1 -= seg[i];
			}
			sum2 += seg[i];
		}
		sum1 = sum1 % 10000;
		sum2 = sum2 % 10000;
		String s = sum1 + "" + sum2;
		hashValue = Math.abs(Integer.parseInt(s) % bucketsLength);
		return hashValue;
	}

}
