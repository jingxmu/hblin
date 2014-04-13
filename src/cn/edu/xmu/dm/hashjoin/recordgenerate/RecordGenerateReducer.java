package cn.edu.xmu.dm.hashjoin.recordgenerate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordGenerateReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {
	private int recordId = 0;
	private final Text newRecord = new Text();
	private final NullWritable nullWritable = NullWritable.get();
	private static String wordSegTokenizer;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		wordSegTokenizer = conf.get("WORD_SEG_TOKENIZER");
	}

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		// Configuration conf=context.getConfiguration();
		// WORDSEPERATOR=conf.get("WORDSEPERATOR");
		newRecord.set(recordId + wordSegTokenizer + key);
		recordId++;
		context.write(newRecord, nullWritable);
	}

}
