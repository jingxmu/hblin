package cn.edu.xmu.dm.hashjoin.recordgenerate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map:为了保留原始文件中的record的顺序，采用一个map对所有record添加id input: key:offset value:record
 * output: key:id:record value:null
 * 
 * @author dm
 * 
 */
public class RecordGenerateMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	private final Text record = new Text();
	private final NullWritable nullWritable = NullWritable.get();
	private long id = 0;
	private String wordSegTokenizer;
	private static Configuration conf;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		conf = context.getConfiguration();
		wordSegTokenizer = conf.get("WORD_SEG_TOKENIZER");

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		record.set(id + wordSegTokenizer + value.toString());
		id++;
		context.write(record, nullWritable);
	}

}
