package cn.edu.xmu.dm.hashjoin.core;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import cn.edu.xmu.dm.hashjoin.calculatesimilarity.CalSignatureSim;
//import cn.edu.xmu.dm.hashjoin.filter.Filter;
import cn.edu.xmu.dm.hashjoin.lsh.LSH;
import cn.edu.xmu.dm.hashjoin.minhashing.HashMatrix;
import cn.edu.xmu.dm.hashjoin.minhashing.MinHashing;
import cn.edu.xmu.dm.hashjoin.minhashing.RecordLengthCount;
import cn.edu.xmu.dm.hashjoin.recordgenerate.RecordGenerate;
import cn.edu.xmu.dm.hashjoin.tokenize.TokenizerDriver;

public class HashJoin {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = Main.getConfiguration(args);
		String stopWords = conf.get("DATA_DIR") + conf.get("STOP_WORD_INPATH");
		DistributedCache.addCacheFile(new Path(stopWords).toUri(), conf);
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		System.out.println(conf.get("DATA_DIR"));
		RecordGenerate.run(conf);
		// Filter.run(conf);
		TokenizerDriver.run(conf);
		String tokensPath = conf.get("DATA_DIR") + conf.get("TOKENS_PATH")
				+ "/part-r-00000";
		DistributedCache.addCacheFile(new Path(tokensPath).toUri(), conf);
		int tokensNum = conf.getInt("tokensNum", 0);
		int hashNum = conf.getInt("hashNum", 400);
		String HASH_MATRIX_PATH = "matrix";

		/* -D 只是改变了conf中相应属性的值，并没有改变字符串DATA_DIR的值，所以必须从conf中取出改变了的值 */
		String matrixPath = conf.get("DATA_DIR") + HASH_MATRIX_PATH;
		HashMatrix.getHashFunction(tokensNum, hashNum, matrixPath, conf);
		DistributedCache.addCacheFile(new Path(matrixPath).toUri(), conf);
		MinHashing.run(conf);
		RecordLengthCount.run(conf);
		String recordLengthPath = conf.get("DATA_DIR")
				+ conf.get("RECORD_LENGTH_PATH") + "/part-r-00000";
		DistributedCache.addCacheFile(new Path(recordLengthPath).toUri(), conf);
		LSH.run(conf);
		String signaturePath = conf.get("DATA_DIR")
				+ conf.get("FEATURE_VECTOR_PATH") + "/part-r-00000";
		DistributedCache.addCacheFile(new Path(signaturePath).toUri(), conf);
		CalSignatureSim.run(conf);

		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / (float) 1000.0
				+ " seconds.");
	}
}
