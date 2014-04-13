package cn.edu.xmu.dm.hashjoin.tokenize;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.xmu.dm.hashjoin.core.Main.MyCounter;

/**
 * reduce:为每个token指定一个int值，之后都用这个int值来代表这个token，而不使用最原始的字符串token
 * input:	key:token(String)	value:null
 * output:	key:token(String)	value:int		
 * @author dm
 *
 */
public class TokenizerReducer extends Reducer<Text,NullWritable,Text,IntWritable> {

	private static IntWritable hashValue=new IntWritable(0);
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,Context context)
			throws IOException, InterruptedException {
		
		context.write(key, hashValue);
		hashValue.set(hashValue.get()+1);
	}

	/**
	 * 为了之后不同的Job使用这个token的总数,需要使用计数器Counter传递这个tokensNum
	 */
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		Counter ct=context.getCounter(MyCounter.TOKENSNUM);
		ct.increment(hashValue.get());
	}
	
	
}
