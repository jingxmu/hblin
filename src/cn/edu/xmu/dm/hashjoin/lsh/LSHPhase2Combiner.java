package cn.edu.xmu.dm.hashjoin.lsh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 对recordIdPair进行计数
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */
public class LSHPhase2Combiner extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private static int sum = 0;
	private static IntWritable count = new IntWritable(0);
	private static Configuration conf;
	private static float threshold;
	private static int hashNum;
	private static int columnsNum;
	private static int gramLength;
	private static int newThreshold;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		threshold = conf.getFloat("threshold", 0.5f);
		hashNum = conf.getInt("hashNum", 100);
		columnsNum = conf.getInt("columnsNum", 5);
		gramLength = conf.getInt("gramLength", 1);
		newThreshold = (int) (2 * threshold * hashNum / ((1 + threshold)
				* columnsNum * gramLength));
		System.out.println("newTheshold=" + newThreshold);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		count.set(sum);
		context.write(key, count);
	}

}
