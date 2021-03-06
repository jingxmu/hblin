package cn.edu.xmu.dm.hashjoin.calculatesimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalSimMapper extends  Mapper<LongWritable,Text,Text,FloatWritable> {
	
	private static Configuration conf;
	private Path[] paths;
	private FileReader fr;
	private BufferedReader br;
	Set<Integer> keySet=new HashSet<Integer>();
	HashSet<Integer> rids;
	HashMap<Integer,String> sig;
	private  final Text ridPair=new Text();
//	private static final Text signature=new Text();
	private FloatWritable similarity=new FloatWritable();
//	private final Text ridPair=new Text();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		conf=context.getConfiguration();
		paths = DistributedCache.getLocalCacheFiles(conf);
		sig=new HashMap<Integer,String>();
		 
		/* 读取哈希签名所在的文件 */
		fr = new FileReader(paths[4].toString());
		br = new BufferedReader(fr);
		String line = br.readLine();
		int rid;
		String s;

		while(line!=null){
			rid=Integer.parseInt(line.split("\t")[0]);
			s=line.split("\t")[1];
			sig.put(rid, s);
			line=br.readLine();
		}
		
	}
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String s=value.toString().split("\t")[0];
		int id1=Integer.parseInt(s.split(",")[0]);
		int id2=Integer.parseInt(s.split(",")[1]);
		String[] record1=sig.get(id1).split(conf.get("WORD_SEPARATOR"));
		String[] record2=sig.get(id2).split(conf.get("WORD_SEPARATOR"));
		int length=record1.length;
		int[] sig1=new int[length];
		int[] sig2=new int[length];
		for(int i=0;i<length;i++){
			sig1[i]=Integer.parseInt(record1[i]);
			sig2[i]=Integer.parseInt(record2[i]);
		}
		float sim = computeSimilarityFromSignatures(sig1, sig2);
		if (sim >= context.getConfiguration().getFloat("threshold", 0)) {
			similarity.set(sim);
			ridPair.set(s);
			context.write(ridPair, similarity);
		}
		similarity.set(sim);
	}

	/**
	 * 计算两个哈希签名的Jaccard相似度
	 * @param record1 记录1的哈希签名组成的数组
	 * @param record2 记录2的哈希签名组成的数组
	 * @return
	 */
	public static float computeSimilarityFromSignatures(int[] record1,
			int[] record2) {
		int numHashFunctions = record1.length;
		int identicalMinHashes = 0;
		for (int i = 0; i < numHashFunctions; i++) {
			if (record1[i] == record2[i]) {
				identicalMinHashes++;
			}
		}
		return (float) ((1.0 * identicalMinHashes) / numHashFunctions);
	}
}
