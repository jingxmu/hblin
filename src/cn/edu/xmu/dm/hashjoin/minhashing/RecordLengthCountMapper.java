package cn.edu.xmu.dm.hashjoin.minhashing;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.xmu.dm.hashjoin.tokenize.ITokenizer;
import cn.edu.xmu.dm.hashjoin.tokenize.TokenizerFactory;

/**
 * 对每条记录进行分词，得到tokens的总数，作为记录的长度，在长度过滤时使用
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */
public class RecordLengthCountMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private IntWritable id = new IntWritable(0);
	private HashSet<String> tokens = new HashSet<String>();
	private static Configuration conf;
	private static String tokenizer;
	private static String wordSeparator;
	private static int gramLength;
	private static String tokenSeparator;
	private static String[] joinAttributesIndex;
	private IntWritable count = new IntWritable(0);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		tokenizer = conf.get("TOKENIZER");
		wordSeparator = conf.get("WORD_SEPARATOR");
		gramLength = conf.getInt("gramLength", 0);
		if (gramLength == 0) {
			throw new RuntimeException("Gram length should be above 0.");
		}
		tokenSeparator = conf.get("TOKEN_SEPARATOR");
		joinAttributesIndex = conf.get("joinAttributesIndex").split(",");
		wordSeparator = conf.get("WORD_SEPARATOR");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] segments = value.toString().split(
				conf.get("WORD_SEG_TOKENIZER"));
		segments[0] = segments[0].trim();
		int recordId = Integer.parseInt(segments[0]);
		id.set(recordId);
		ITokenizer t = TokenizerFactory.getTokenizer(tokenizer, gramLength,
				wordSeparator, tokenSeparator);
		List<String> tokenOfRecord = t.tokenize(getJoinAttributes(value
				.toString()));
		tokens.clear();
		for (String token : tokenOfRecord) {
			tokens.add(token);
		}
		count.set(tokens.size());
		context.write(id, count);
	}

	public static String getJoinAttributes(String text) {

		int[] indexs = new int[joinAttributesIndex.length];
		text = text.toLowerCase();
		for (int i = 0; i < joinAttributesIndex.length; i++) {
			indexs[i] = Integer.parseInt(joinAttributesIndex[i]);
		}
		StringBuffer joinAttributes = new StringBuffer();
		String[] segments = text.split(conf.get("WORD_SEG_TOKENIZER"));
		// joinAttributes.append(segments[0]+wordSeparator);
		for (int i = 0; i < indexs.length; i++) {
			if (indexs[i] > segments.length) {
				throw new RuntimeException("join attributes indexs \""
						+ joinAttributesIndex[i] + "\" has been out of bounds.");
			}
			joinAttributes.append(segments[indexs[i]] + wordSeparator);
		}
		return joinAttributes.toString();
	}
}
