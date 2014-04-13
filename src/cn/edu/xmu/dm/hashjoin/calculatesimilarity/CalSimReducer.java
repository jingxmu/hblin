package cn.edu.xmu.dm.hashjoin.calculatesimilarity;

import java.io.IOException;

//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 得到record id pair中的两个record的signature，并且计算它们的相似度，返回大于阈值的相似度
 * input:	key:record id pair	value:signature1  signature2
 * ouput:	key:record id pair	value:similarity of signatures
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */
public class CalSimReducer extends
		Reducer<Text, Text, Text, Text> {

//	private FloatWritable similarity = new FloatWritable(0);
	private Text text=new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int[] record1 = null;
		int[] record2 = null;
		String[] temp;
		boolean flag = true;
		float sim = 0;
		
		/* 得到两条记录的哈希签名向量 */
		for (Text sig : values) {
			if(sig.charAt(0)=='s'){
				sim=Float.parseFloat(sig.toString().substring(1));
				if (sim >= context.getConfiguration().getFloat("threshold", 0)) {
//					similarity.set(sim);
					text.set(sim+"");
					context.write(key, text);
				}
				flag=false;
				break;
			}
			else{
				temp = sig.toString().split(
						context.getConfiguration().get("WORD_SEPARATOR"));
				if (flag) {
					record1 = new int[temp.length];
					for (int i = 0; i < temp.length; i++) {
						record1[i] = Integer.parseInt(temp[i]);
					}
					flag = false;
				} else {
					record2 = new int[temp.length];
					for (int i = 0; i < temp.length; i++) {
						record2[i] = Integer.parseInt(temp[i]);
					}
					flag = true;
				}
			}			
			
		}
		if(flag == true){
			sim = computeSimilarityFromSignatures(record1, record2);
			System.out.println("in "+sim);
			if (sim >= context.getConfiguration().getFloat("threshold", 0)) {
				text.set(sim+"");
				context.write(key, text);
			}
		}
		
		
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
