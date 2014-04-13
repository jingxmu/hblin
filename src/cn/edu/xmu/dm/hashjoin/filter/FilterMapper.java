package cn.edu.xmu.dm.hashjoin.filter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * setup:读取分布式缓存中的停用词，
 * map:用取到的停用词对源文件进行过滤
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */

public class FilterMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

	private static Collection<String> stopWords=new HashSet<String>();
	private static Configuration conf;
	private static  String wordSeparator;
	private final Text outValue=new Text();
	private final NullWritable nullWritable=NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		
		conf=context.getConfiguration();
		wordSeparator=conf.get("WORD_SEPARATOR");
		
		/*读取分布式缓存中的stop words*/
		Path[] path=DistributedCache.getLocalCacheFiles(conf);
		FileReader fr=new FileReader(path[0].toString());
		BufferedReader br=new BufferedReader(fr);
		String word=br.readLine();
		while(word!=null){
			stopWords.add(word);
			word=br.readLine();
		}
		br.close();
	}
	

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String record=value.toString();
		record=record.toLowerCase();
		String[] tokensSegment;
		String[] tokens;
		final StringBuffer newRecord=new StringBuffer();
		
		/*标点符号统一换成空格*/
		Pattern rePunctuation = Pattern.compile("[^:\\p{L}\\p{N}]");
		record=rePunctuation.matcher(record).replaceAll(wordSeparator);
		tokensSegment=record.split(conf.get("WORD_SEG_TOKENIZER"));
		for(int i=0;i<tokensSegment.length;i++){
			tokens=tokensSegment[i].split(wordSeparator);			
			for(int j=0;j<tokens.length;j++){
				if(!stopWords.contains(tokens[j])){
					newRecord.append(tokens[j]+wordSeparator);
				}
			}
			newRecord.append(conf.get("WORD_SEG_TOKENIZER"));
		}
		newRecord.delete(newRecord.length()-2, newRecord.length());
		outValue.set(newRecord.toString());
		context.write(outValue,nullWritable);
	}
}
