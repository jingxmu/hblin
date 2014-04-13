package cn.edu.xmu.dm.hashjoin.lsh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reduce:对相同的record id pair统计其出现的次数， 可选操作：如果次数大于新的阈值，就作为真正的候选对，完成二次过滤 input:
 * key:record id pair value:count output: key:record id value:count（实际没有用处）
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */

public class LSHPhase2Reducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private static int sum = 0;
	private static IntWritable count = new IntWritable(0);
	private static Configuration conf;
	private static float threshold;
	private static int hashNum;
	private static int columnsNum;
	private static int newThreshold;
	private static boolean secondaryfilter;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		threshold = conf.getFloat("threshold", 0.5f);
		hashNum = conf.getInt("hashNum", 100);
		columnsNum = conf.getInt("columnsNum", 5);
		newThreshold = (int) (hashNum * threshold / columnsNum);
		secondaryfilter = conf.getBoolean("secondaryfilter", false);
		// System.out.println("newTheshold="+newThreshold);

	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		if (secondaryfilter) {
			if (sum >= newThreshold) {
				count.set(sum);
				context.write(key, count);
			}
		} else {
			count.set(sum);
			context.write(key, count);
		}
	}
}
