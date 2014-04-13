package cn.edu.xmu.dm.hashjoin.core;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ProgramDriver;

import cn.edu.xmu.dm.hashjoin.calculatesimilarity.CalSignatureSim;
import cn.edu.xmu.dm.hashjoin.filter.Filter;
import cn.edu.xmu.dm.hashjoin.lsh.LSH;
import cn.edu.xmu.dm.hashjoin.minhashing.HashMatrix;
import cn.edu.xmu.dm.hashjoin.minhashing.MinHashing;
import cn.edu.xmu.dm.hashjoin.minhashing.RecordLengthCount;
import cn.edu.xmu.dm.hashjoin.recordgenerate.RecordGenerate;
import cn.edu.xmu.dm.hashjoin.tokenize.TokenizerDriver;

/**
 * 1.声明需要用到的一些全局参数，将这些参数通过Configuration类的set方法设置，使得所有Job能共享
 * 2.获得每个Job需要的configuration
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */
public class Main {

	public static final String WORD_SEG_TOKENIZER = ":";
	public static final String WORD_SEPARATOR = " ";
	public static final String DATA_DIR_NAME = "DATA_DIR";
	public static final String DATA_DIR = "hdfs://localhost:9000/user/dm/hb100/";
	public static final String DATA_RAW_INPUT = "raw";// ////
	public static final String RAW_WITH_ID = "rawWithId";
	public static final String STOP_WORD_INPATH = "stopwords";
	public static final String NEW_RAW_PATH = "rawAfterFilter";
	public static final String TOKENS_PATH = "tokens";
	public static final String HASH_MATRIX_PATH = "matrix";
	public static final String FEATURE_VECTOR_PATH = "signature";
	public static final String RECORD_LENGTH_PATH = "recordlength";
	public static final String LSH_PHASE1_PATH = "lshPhase1";
	public static final String LSH_PHASE2_PATH = "candidates";
	public static final String CALCULATE_PHASE1_PATH = "calPhase1";
	public static final String SIMILARITY_PATH = "final_result";

	public static final String WORD_TOKENIZER = " ";
	public static final String TOKENIZER = "NWord";
	public static final String TOKEN_SEPARATOR = "_";
	public static int gramLength = 1;
	public static String joinAttributesIndex = "1";
	public static boolean filtering = false;
	public static float threshold = 0.8f;
	public static int hashNum = 100;
	public static int bucketsLength = 1000000001;
	public static int columnsNum = 10;
	public static int reduceNum=1;
	public static int bucketsNum = hashNum / columnsNum;
	public static int tokensNum = 0;

	public static enum MyCounter {
		TOKENSNUM
	}

	public static void main(String[] args) throws Throwable {
		ProgramDriver pd = new ProgramDriver();
		pd.addClass("1recordgen", RecordGenerate.class, "");
		pd.addClass("2filter", Filter.class, "");
		pd.addClass("3tokenizer", TokenizerDriver.class, "tokenizer.");
		pd.addClass("4minhash", MinHashing.class, "");
		pd.addClass("5recordlencount", RecordLengthCount.class,
				"RecordLengthCount.");
		pd.addClass("6lsh", LSH.class, "");
		pd.addClass("7calcsim", CalSignatureSim.class, "");
		pd.addClass("hashjoin", HashJoin.class, "");
		pd.addClass("hashjoinvector", HashJoinVector.class, "");
		pd.driver(args);
		// runMain(args);
	}

	public static void runMain(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = getConfiguration(args);
		new GenericOptionsParser(conf, args);
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);

		RecordGenerate.run(conf);
		Filter.run(conf);
		TokenizerDriver.run(conf);
		String tokensPath = conf.get("DATA_DIR") + conf.get("TOKENS_PATH")
				+ "/part-r-00000";
		DistributedCache.addCacheFile(new Path(tokensPath).toUri(), conf);
		tokensNum = conf.getInt("tokensNum", 0);
		hashNum = conf.getInt("hashNum", hashNum);
		String matrixPath = conf.get("DATA_DIR") + HASH_MATRIX_PATH;
		// -D 只是改变了conf中相应属性的值，并没有改变字符串DATA_DIR的值，所以必须从conf中取出改变了的值

		HashMatrix.getHashFunction(tokensNum, hashNum, matrixPath, conf);
		// System.out.println(new Path(matrixPath).toUri());
		DistributedCache.addCacheFile(new Path(matrixPath).toUri(), conf);
		MinHashing.run(conf);
		RecordLengthCount.run(conf);
		String recordLengthPath = conf.get("DATA_DIR")
				+ conf.get("RECORD_LENGTH_PATH") + "/part-m-00000";
		DistributedCache.addCacheFile(new Path(recordLengthPath).toUri(), conf);
		LSH.run(conf);
		CalSignatureSim.run(conf);

		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / (float) 1000.0
				+ " seconds.");
	}

	public static Configuration getConfiguration(String[] args) {
		Configuration conf = new Configuration();
		// conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		// conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("default.xml"));
//		conf.set("DATA_RAW_INPUT", DATA_RAW_INPUT);
//		conf.set("DATA_DIR", DATA_DIR);
//		conf.set("RAW_WITH_ID", RAW_WITH_ID);
//		conf.set("STOP_WORD_INPATH", STOP_WORD_INPATH);
//		conf.set("NEW_RAW_PATH", NEW_RAW_PATH);
//		conf.set("TOKENS_PATH", TOKENS_PATH);
//		conf.set("FEATURE_VECTOR_PATH", FEATURE_VECTOR_PATH);
//		conf.set("RECORD_LENGTH_PATH", RECORD_LENGTH_PATH);
//		conf.set("LSH_PHASE1_PATH", LSH_PHASE1_PATH);
//		conf.set("LSH_PHASE2_PATH", LSH_PHASE2_PATH);
//		conf.set("SIMILARITY_PATH", SIMILARITY_PATH);
//		conf.set("WORD_SEG_TOKENIZER", WORD_SEG_TOKENIZER);
//		conf.set("WORD_SEPARATOR", WORD_SEPARATOR);
//		conf.set("TOKENIZER", TOKENIZER);
//		conf.setInt("gramLength", gramLength);
//		conf.set("TOKEN_SEPARATOR", TOKEN_SEPARATOR);
//		conf.setBoolean("filtering", filtering);
//		conf.set("joinAttributesIndex", joinAttributesIndex);
//		conf.setInt("hashNum", hashNum);
//		if (hashNum % columnsNum != 0) {
//			throw new RuntimeException("hashNum should be time of columnsNum");
//		}
//		conf.setInt("columnsNum", columnsNum);
//		conf.setInt("bucketsLength", bucketsLength);
//		conf.setFloat("threshold", threshold);
//
//		conf.setInt("tokensNum", tokensNum);
//		conf.set("CALCULATE_PHASE1_PATH", CALCULATE_PHASE1_PATH);
//		conf.set("HASH_MATRIX_PATH", HASH_MATRIX_PATH);
		new GenericOptionsParser(conf, args);
		return conf;
	}

	public static Configuration getConAfterMinHashed(String[] args)
			throws IOException {
		Configuration conf = getConfiguration(args);

		String tokensPath = conf.get("DATA_DIR") + conf.get("TOKENS_PATH")
				+ "/part-r-00000";
		String stopWords = conf.get("DATA_DIR") + conf.get("STOP_WORD_INPATH");
		DistributedCache.addCacheFile(new Path(stopWords).toUri(), conf);
		DistributedCache.addCacheFile(new Path(tokensPath).toUri(), conf);
		int tokensNum = conf.getInt("tokensNum", 0);
//		System.out.println("tokensNum: " + tokensNum);
		int hashNum = conf.getInt("hashNum", Main.hashNum);
		String matrixPath = conf.get("DATA_DIR") + Main.HASH_MATRIX_PATH;
		// -D 只是改变了conf中相应属性的值，并没有改变字符串DATA_DIR的值，所以必须从conf中取出改变了的值

		HashMatrix.getHashFunction(tokensNum, hashNum,matrixPath, conf);
		DistributedCache.addCacheFile(new Path(matrixPath).toUri(), conf);
		return conf;
	}

	public static Configuration getConfAfterRecordLenCounted(String[] args)
			throws IOException {
		Configuration conf = getConfiguration(args);

		String tokensPath = conf.get("DATA_DIR") + conf.get("TOKENS_PATH")
				+ "/part-r-00000";
		String stopWords = conf.get("DATA_DIR") + conf.get("STOP_WORD_INPATH");
		DistributedCache.addCacheFile(new Path(stopWords).toUri(), conf);
		DistributedCache.addCacheFile(new Path(tokensPath).toUri(), conf);
		String matrixPath = conf.get("DATA_DIR") + Main.HASH_MATRIX_PATH;
		DistributedCache.addCacheFile(new Path(matrixPath).toUri(), conf);
		String recordLengthPath = conf.get("DATA_DIR")
				+ conf.get("RECORD_LENGTH_PATH") + "/part-r-00000";
		DistributedCache.addCacheFile(new Path(recordLengthPath).toUri(), conf);
		String signaturePath = conf.get("DATA_DIR")
				+ conf.get("FEATURE_VECTOR_PATH") + "/part-r-00000";
		DistributedCache.addCacheFile(new Path(signaturePath).toUri(), conf);
		return conf;
	}
}
