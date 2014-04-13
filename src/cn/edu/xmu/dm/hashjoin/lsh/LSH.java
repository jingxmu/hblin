package cn.edu.xmu.dm.hashjoin.lsh;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import cn.edu.xmu.dm.hashjoin.core.Main;


public class LSH {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//		TokenizerDriver.main(null);
		Configuration conf=Main.getConfAfterRecordLenCounted(args);
		run(conf);
	}
	public static void run(Configuration conf) 
			throws IOException, InterruptedException, ClassNotFoundException{
//		Configuration conf=Main.getConfiguration();
		Job job1 = new Job(conf,"LSHPhase1Job");
		job1.setJarByClass(LSH.class);
		job1.setMapperClass(LSHPhase1Mapper.class);
		job1.setReducerClass(LSHPhase1Reducer.class);
		job1.setNumReduceTasks(conf.getInt("reduceNum", 1));
		job1.setGroupingComparatorClass(SegmentGroup.class);
		job1.setPartitionerClass(SegmentPartitioner.class);
		job1.setSortComparatorClass(KeyComparator.class);
		job1.setMapOutputKeyClass(IntPair.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		String dataDir = conf.get("DATA_DIR");
		if (dataDir == null) {
			System.err.println("ERROR: data.dir not set");
			System.exit(-1);
		}
		String inSuffix;
		inSuffix = conf.get("FEATURE_VECTOR_PATH");
		if (inSuffix.isEmpty()) {
			System.err.println("ERROR: signature path not set");
			System.exit(-1);		
		}
		
		String outSuffix=conf.get("LSH_PHASE1_PATH");
		
		FileInputFormat.addInputPath(job1, new Path(dataDir + inSuffix));
		Path outputPath = new Path(dataDir+outSuffix);
		FileOutputFormat.setOutputPath(job1, outputPath);
		FileSystem.get(conf).delete(outputPath, true);

		runJob(job1);
		
		Job job2 = new Job(conf,"LSHPhase2Job");
		job2.setJarByClass(LSH.class);
		job2.setMapperClass(LSHPhase2Mapper.class);
		job2.setReducerClass(LSHPhase2Reducer.class);
		job2.setNumReduceTasks(1);
		job2.setCombinerClass(LSHPhase2Combiner.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		inSuffix = conf.get("LSH_PHASE1_PATH");
		if (inSuffix.isEmpty()) {
			System.err.println("ERROR: lsh phase1 path not set");
			System.exit(-1);		
		}
		
		outSuffix=conf.get("LSH_PHASE2_PATH");
		
		FileInputFormat.addInputPath(job2, new Path(dataDir + inSuffix+"/part-r-*"));
		outputPath = new Path(dataDir+outSuffix);
		FileOutputFormat.setOutputPath(job2, outputPath);
		FileSystem.get(conf).delete(outputPath, true);
		runJob(job2);


	}
	public static void runJob(Job job) 
			throws IOException, InterruptedException, ClassNotFoundException{
		String ret = "LSH(" + job.getJobName() + ")\n"
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
