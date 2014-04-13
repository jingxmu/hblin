package cn.edu.xmu.dm.hashjoin.lsh;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 将输入的recordIdPair count直接输出，在老版本的API中可以使用IdentityMapper类实现该功能
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */

public class LSHPhase2Mapper extends
		Mapper<Text, IntWritable, Text, IntWritable> {

	/**
	 * 不实用offset作为输入的key，而直接用Text类，是因为输入使用了SequenceFileInputFormat 这提高了处理的效率
	 */
	@Override
	protected void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {

		context.write(key, value);
	}

}
