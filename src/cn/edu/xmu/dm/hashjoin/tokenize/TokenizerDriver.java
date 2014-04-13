package cn.edu.xmu.dm.hashjoin.tokenize;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.xmu.dm.hashjoin.core.Main;
import cn.edu.xmu.dm.hashjoin.core.Main.MyCounter;

/**
 * 分词驱动类
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */
public class TokenizerDriver {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = Main.getConfiguration(args);
		run(conf);
	}

	/**
	 * 设置分词Job所需要的参数，并且显示运行时间信息
	 * 
	 * @param pConf 包含Job运行所需要的全局变量值
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void run(Configuration pConf) throws IOException,
			InterruptedException, ClassNotFoundException {
		// Configuration conf=Main.getConfiguration();
		Long splitSize = pConf.getLong("mapred.min.split.size", 0);
		pConf.setLong("mapred.min.split.size", Long.MAX_VALUE);
		Job job = new Job(pConf, "TokenizerJob");
		job.setJarByClass(TokenizerDriver.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TokenizerReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		String dataDir = pConf.get("DATA_DIR");
		if (dataDir == null) {
			System.err.println("ERROR: data.dir not set");
			System.exit(-1);
		}

		boolean isFiltering = pConf.getBoolean("filtering", false);
		String inSuffix;
		if (isFiltering) {
			inSuffix = pConf.get("NEW_RAW_PATH");
			if (inSuffix.isEmpty()) {
				System.err.println("ERROR: raw file after filter not set");
				System.exit(-1);
			}
		} else {
			inSuffix = pConf.get("RAW_WITH_ID");
			if (inSuffix.isEmpty()) {
				System.err.println("ERROR: raw file with id not set");
				System.exit(-1);
			}
		}
		String outSuffix = pConf.get("TOKENS_PATH");

		FileInputFormat.addInputPath(job, new Path(dataDir + inSuffix));
		Path outputPath = new Path(dataDir + outSuffix);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(pConf).delete(outputPath, true);

		String ret = "Tokenizer(" + job.getJobName() + ")\n"
				+ "  Input Path:  {";
		Path inputs[] = FileInputFormat.getInputPaths(job);
		for (int ctr = 0; ctr < inputs.length; ctr++) {
			if (ctr > 0) {
				ret += "\n                ";
			}
			ret += inputs[ctr].toString();
		}
		ret += "}\n";
		ret += "  Output Path: " + FileOutputFormat.getOutputPath(job) + "\n"
				+ "  Reduce Jobs: " + job.getNumReduceTasks() + "\n";
		System.out.println(ret);
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / (float) 1000.0
				+ " seconds.");
		Counters cts = job.getCounters();
		Counter ct = cts.findCounter(MyCounter.TOKENSNUM);
		System.out.println("tokensNum:" + (int) ct.getValue());
		pConf.setInt("tokensNum", (int) ct.getValue());
		pConf.setLong("mapred.min.split.size", splitSize);
	}
}
