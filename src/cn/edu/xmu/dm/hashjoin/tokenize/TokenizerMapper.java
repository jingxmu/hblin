package cn.edu.xmu.dm.hashjoin.tokenize;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map:对每条记录抽取出其中的连接属性值，并且用分词器对其进行分词，最终得到所有的token
 * input:   value:record
 * output:  key:token value:null
 * @author dm
 *
 */

public class TokenizerMapper extends  Mapper<LongWritable,Text,Text,NullWritable> {
	
	private final Text token=new Text();
	private final NullWritable nullWritable=NullWritable.get();
	private static Configuration conf;
	private static String tokenizer;
	private static String wordSeparator;
	private static int gramLength;
	private static String tokenSeparator;
	private static String[] joinAttributesIndex;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf=context.getConfiguration();
		tokenizer=conf.get("TOKENIZER");
		wordSeparator=conf.get("WORD_SEPARATOR");
		gramLength=conf.getInt("gramLength", 0);
		if(gramLength==0){
			throw new RuntimeException("Gram length should be above 0.");
		}
		tokenSeparator=conf.get("TOKEN_SEPARATOR");
		joinAttributesIndex=conf.get("joinAttributesIndex").split(",");
	}


	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		ITokenizer t=TokenizerFactory.getTokenizer(
				tokenizer, gramLength, wordSeparator, tokenSeparator);
		
		List<String> tokens=t.tokenize(getJoinAttributes(value.toString()));
//		System.out.println(value.toString());
		for(String aToken:tokens){
			token.set(aToken);
//			System.out.println(token.toString());
			context.write(token, nullWritable);
		}
	}
	
	public static String getJoinAttributes(String text){
		
		int[] indexs=new int[joinAttributesIndex.length];
		StringBuffer joinAttributes=new StringBuffer();
		text=text.toLowerCase();
//		System.out.println(text);
		String[] segments=text.split(conf.get("WORD_SEG_TOKENIZER"));
		if(segments.length==1){
			System.out.println(text);
		}
		for(int i=0;i<joinAttributesIndex.length;i++){
			indexs[i]=Integer.parseInt(joinAttributesIndex[i]);
		}
//		joinAttributes.append(segments[0]+wordSeparator);
		for(int i=0;i<indexs.length;i++){
			if(indexs[i]>segments.length){
				throw new RuntimeException("join attributes indexs \"" + joinAttributesIndex[i] + "\" has been out of bounds."); 
			}
			joinAttributes.append(segments[indexs[i]]+wordSeparator);
		}
		return joinAttributes.toString();
	}
	
}
