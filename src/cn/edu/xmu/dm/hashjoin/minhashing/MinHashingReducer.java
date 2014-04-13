package cn.edu.xmu.dm.hashjoin.minhashing;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 为每条record生成最小哈希签名 input: key:offset value:recordId tokenId1,tokenId2,...
 * output: key:recordId value:1*hashNum的向量，写成String格式
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */

public class MinHashingReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	private static Configuration conf;
	private int[] tokensId;
	private Text signature = new Text();
	private static String wordSeparator;
	private static int hashNum;
	private static int tokensNum;
	private FileInputStream fs;
	private DataInputStream dis;
	private Path[] paths;
	private int[][] hashArguments;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		conf = context.getConfiguration();
		wordSeparator = conf.get("WORD_SEPARATOR");
		hashNum = conf.getInt("hashNum", 0);
		hashArguments = new int[hashNum][2];
		if (hashNum == 0) {
			throw new RuntimeException("hash num is wrong");
		}
		tokensNum = conf.getInt("tokensNum", 0);
		paths = DistributedCache.getLocalCacheFiles(conf);

		/*
		 * paths[2]中存放的是ai+b中的参数a,b 文件格式为：a b
		 */
		fs = new FileInputStream(paths[2].toString());
		dis = new DataInputStream(fs);
		for (int i = 0; i < hashNum; i++) {
			hashArguments[i][0] = dis.readInt();
			hashArguments[i][1] = dis.readInt();
		}
		dis.close();
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		String[] tokensInString;
		int[] signatureInt = new int[hashNum];
		String temp = "";
		int hashValue;
		for (Text record : value) {
			temp = "";
			tokensInString = record.toString().split(wordSeparator);
			tokensId = new int[tokensInString.length];
			for (int i = 0; i < tokensInString.length; i++) {
				tokensId[i] = Integer.parseInt(tokensInString[i]);
			}

			/* MinHashing算法的实现 */
			for (int h = 0; h < hashNum; h++) {
				signatureInt[h] = Integer.MAX_VALUE;
				for (int id : tokensId) {
					hashValue = Math
							.abs((hashArguments[h][0] * id + hashArguments[h][1])
									% tokensNum);
					if (signatureInt[h] > hashValue) {
						signatureInt[h] = hashValue;
					}
				}
				temp += signatureInt[h] + wordSeparator;
			}

			signature.set(temp);
			context.write(key, signature);
		}

	}

}
