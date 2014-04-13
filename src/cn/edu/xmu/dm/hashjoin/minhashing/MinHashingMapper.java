package cn.edu.xmu.dm.hashjoin.minhashing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.xmu.dm.hashjoin.tokenize.ITokenizer;
import cn.edu.xmu.dm.hashjoin.tokenize.TokenizerFactory;

/**
 * map:对每条record进行分词得到token，并且用整数来表示这些token input: key:offset value:record
 * output: key:record id value:token1 token2 ...
 * 
 * @author dm
 * 
 */
public class MinHashingMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private IntWritable id = new IntWritable(0);
	private HashSet<String> tokens = new HashSet<String>();
	private final Text tokensToString = new Text();
	private static Configuration conf;
	private static String tokenizer;
	private static String wordSeparator;
	private static int gramLength;
	private static String tokenSeparator;
	private static String[] joinAttributesIndex;
	private FileReader fr;
	private BufferedReader br;
	private Path[] paths;
	private HashMap<String, Integer> tokenId = new HashMap<String, Integer>();

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
		paths = DistributedCache.getLocalCacheFiles(conf);
//		for (int i = 0; i < paths.length; i++) {
//			System.out.println(paths[i].getName());
//		}

		/* 分布式缓存文件paths[1]中存放了之前产生的所有的token，先需要将其读入，在对记录进行分词之后映射时使用 */
		fr = new FileReader(paths[1].toString());
		br = new BufferedReader(fr);
		String line = br.readLine();

		/* 文件的格式是： token tokenId,如 hello 1 */
		String[] tokenAndId = line.split("\t");
		while (line != null) {
			tokenId.put(tokenAndId[0], Integer.parseInt(tokenAndId[1]));
			line = br.readLine();
			if (line != null) {
				tokenAndId = line.split("\t");
			}
		}
		br.close();
	}

	/**
	 * 对每条record进行分词产生tokens，并且用tokenId来表示这些tokens,从而将记录用tokenId组成的集合表示
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] segments = value.toString().split(
				conf.get("WORD_SEG_TOKENIZER"));
		segments[0] = segments[0].trim();
		int recordId = Integer.parseInt(segments[0]);
		String idsOfRecord = "";
		id.set(recordId);
		ITokenizer t = TokenizerFactory.getTokenizer(tokenizer, gramLength,
				wordSeparator, tokenSeparator);
		List<String> tokenOfRecord = t.tokenize(getJoinAttributes(value
				.toString()));
		tokens.clear();
		for (String token : tokenOfRecord) {
			tokens.add(token);
		}
		for (String token : tokens) {
			idsOfRecord += tokenId.get(token) + wordSeparator;
//			idsOfRecord += token + wordSeparator;
		}
		if (!idsOfRecord.equals("")) {
			tokensToString.set(idsOfRecord);
			context.write(id, tokensToString);
		}
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
