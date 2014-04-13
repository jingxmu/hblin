package cn.edu.xmu.dm.hashjoin.calculatesimilarity;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.xmu.dm.hashjoin.core.Main;

/**
 * 设置计算相似度所需的参数
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */
public class CalVectorSim {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = Main.getConfAfterRecordLenCounted(args);
		run(conf);
	}

	public static void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = new Job(conf, "CalSimJob");
		job.setJarByClass(CalVectorSim.class);
		job.setMapperClass(CalVectorSimMapper.class);
//		job.setCombinerClass(CalSimCombiner.class);
//		job.setReducerClass(CalSimReducer.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		String dataDir = conf.get("DATA_DIR");
		if (dataDir == null) {
			System.err.println("ERROR: data.dir not set");
			System.exit(-1);
		}
		String inSuffix = conf.get("LSH_PHASE2_PATH");
		if (inSuffix.isEmpty()) {
			System.err.println("ERROR: signature file not set");
			System.exit(-1);
		}
		FileInputFormat.addInputPath(job, new Path(dataDir + inSuffix));

		String outSuffix = conf.get("SIMILARITY_PATH");
		Path outputPath = new Path(dataDir+outSuffix);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(conf).delete(outputPath, true);

		runJob(job);
	}

	public static void runJob(Job job) throws IOException,
			InterruptedException, ClassNotFoundException {
		String ret = "CalSignatureSim(" + job.getJobName() + ")\n"
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
	}
}
