package cn.edu.xmu.dm.hashjoin.lsh;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 生成候选对 input: key:自定义的IntPair value:recordId output: key:候选的record id pair
 * value:1 output中的value主要用于二次过滤
 * 
 * @author dm
 * 
 */
public class LSHPhase1Reducer extends
		Reducer<IntPair, IntWritable, Text, IntWritable> {

	private HashSet<Integer> rids = new HashSet<Integer>();
	private Text ridPairs = new Text();
	private IntWritable one = new IntWritable(1);
	private FileReader fr;
	private BufferedReader br;
	private Path[] paths;
	private HashMap<Integer, Integer> recordLength = new HashMap<Integer, Integer>();
	private static Configuration conf;
	private static float threshold;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		threshold = conf.getFloat("threshold", 0.5f);
		paths = DistributedCache.getLocalCacheFiles(conf);
		for (int i = 0; i < paths.length; i++) {
			System.out.println(paths[i].getName());
		}

		/* 读取记录长度所在的文件，用于长度过滤 */
		fr = new FileReader(paths[3].toString());
		br = new BufferedReader(fr);
		String line = br.readLine();
		int recordId, length;

		while (line != null) {
			recordId = Integer.parseInt(line.split("\t")[0]);
			length = Integer.parseInt(line.split("\t")[1]);
			recordLength.put(recordId, length);
			line = br.readLine();
		}
	}

	@Override
	protected void reduce(IntPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		/* 由于使用到了二次排序，因此可以根据不同的key值来区分不同的桶 */
		String key1, key2;
		key1 = key.toString();
		rids.clear();
		Iterator<IntWritable> it = values.iterator();
		int recordId, length1, length2;
		while (it.hasNext()) {
			key2 = key.toString();
			recordId = it.next().get();
			if (key1.equals(key2)) {
				rids.add(recordId);
			} else {
				if (rids.size() >= 2) {
					
					/*对每个桶中出现的所有recordId，两两作为一个候选对*/
					for (Integer id1 : rids) {
						for (Integer id2 : rids) {
							if (id1 != id2) {
								ridPairs.set(Math.min(id1, id2) + ","
										+ Math.max(id1, id2));
								length1 = recordLength.get(id1);
								length2 = recordLength.get(id2);
								
								/*长度过滤*/
								if (1.0 * Math.min(length1, length2)
										/ Math.max(length1, length2) >= threshold) {
									context.write(ridPairs, one);
								}
							}
						}
					}
				}

				/* 查看每个intPair中所对应的recordId数，由此可以大概估计产生的候选对数 */
				// ridPairs.set(key1);
				// one.set(rids.size());
				// context.write(ridPairs,one );

				rids.clear();
				rids.add(recordId);
				key1 = key2;
			}

		}
	}

}
