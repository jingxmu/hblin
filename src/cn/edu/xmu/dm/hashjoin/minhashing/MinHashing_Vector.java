package cn.edu.xmu.dm.hashjoin.minhashing;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.xmu.dm.hashjoin.core.Main;

public class MinHashing_Vector {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = Main.getConAfterMinHashed(args);
		MinHashing_Vector.run(conf);
	}

	public static void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = new Job(conf, "MinHashing_VectorJob");
		job.setJarByClass(MinHashing_Vector.class);
		job.setMapperClass(MinHashingMapper.class);
//		job.setReducerClass(MinHashingReducer.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		String dataDir = conf.get("DATA_DIR");
		if (dataDir == null) {
			System.err.println("ERROR: data.dir not set");
			System.exit(-1);
		}
		boolean isFiltering = conf.getBoolean("filtering", false);
		String inSuffix;
		if (isFiltering) {
			inSuffix = conf.get("NEW_RAW_PATH");
			if (inSuffix.isEmpty()) {
				System.err.println("ERROR: raw file after filter not set");
				System.exit(-1);
			}
		} else {
			inSuffix = conf.get("RAW_WITH_ID");
			if (inSuffix.isEmpty()) {
				System.err.println("ERROR: raw file with id not set");
				System.exit(-1);
			}
		}
		String outSuffix = conf.get("FEATURE_VECTOR_PATH");

		FileInputFormat.addInputPath(job, new Path(dataDir + inSuffix));
		Path outputPath = new Path(dataDir + outSuffix);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(conf).delete(outputPath, true);

		String ret = "MinHashing(" + job.getJobName() + ")\n"
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
