package cn.edu.xmu.dm.hashjoin.minhashing;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 该Job可以不使用Reduce函数，可以通过控制只有一个map函数达到相同的目的，但是在大数据时出错了，产生不只一个输出文件？
 * 所以通过控制只有一个reduce函数保证只有一个输出文件，加入到分布式缓存中以供使用
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */
public class RecordLengthCountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		context.write(key, values.iterator().next());
	}

}
