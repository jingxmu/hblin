package cn.edu.xmu.dm.hashjoin.recordgenerate;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.xmu.dm.hashjoin.core.Main;

/**
 * 为每条记录添加id，在之后的处理中使用这个id代表一条记录
 * 
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 * 
 */
public class RecordGenerate {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = Main.getConfiguration(args);
		run(conf);
	}

	/**
	 * 设置添加id Job所需要的参数，并打印时间
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		Long splitSize = conf.getLong("mapred.min.split.size", 0);
		conf.setLong("mapred.min.split.size", Long.MAX_VALUE);
		Job job = new Job(conf, "RecordGenerateJob");
		job.setJarByClass(RecordGenerate.class);
		job.setMapperClass(RecordGenerateMapper.class);
		// job.setReducerClass(RecordGenerateReducer.class);

		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		String dataDir = conf.get("DATA_DIR");
		if (dataDir == null) {
			System.err.println("ERROR: data.dir not set");
			System.exit(-1);
		}
		String inSuffix = conf.get("DATA_RAW_INPUT");
		if (inSuffix.isEmpty()) {
			System.err.println("ERROR: raw file not set");
			System.exit(-1);
		}
		String outSuffix = conf.get("RAW_WITH_ID");
		FileInputFormat.addInputPath(job, new Path(dataDir + inSuffix));
		Path outputPath = new Path(dataDir + outSuffix);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(conf).delete(outputPath, true);

		String ret = "RecordGenerate(" + job.getJobName() + ")\n"
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

		conf.setLong("mapred.min.split.size", splitSize);
	}
}
