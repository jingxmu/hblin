package cn.edu.xmu.dm.hashjoin.filter;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.xmu.dm.hashjoin.core.Main;

/**
 * 对原始文件进行过滤，过滤掉其中的停用词
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */

public class Filter {

	public static void main(String[] args)
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = Main.getConfiguration(args);
		run(conf);
	}
	public static void run(Configuration conf) 
			throws IOException, InterruptedException, ClassNotFoundException{

		String stopWords=conf.get("DATA_DIR")+conf.get("STOP_WORD_INPATH");
		DistributedCache.addCacheFile(new Path(stopWords).toUri(), conf);
		Job job = new Job(conf,"FilterJob");
		job.setJarByClass(Filter.class);
		job.setMapperClass(FilterMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		String dataDir = conf.get("DATA_DIR");
		if (dataDir == null) {
			System.err.println("ERROR: data.dir not set");
			System.exit(-1);
		}
		String inSuffix = conf.get("RAW_WITH_ID");
		if (inSuffix.isEmpty()) {
			System.err.println("ERROR: raw file with id not set");
			System.exit(-1);		
		}
		String outSuffix=conf.get("NEW_RAW_PATH");
		
		FileInputFormat.addInputPath(job, new Path(dataDir + inSuffix));
		Path outputPath = new Path(dataDir+outSuffix);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(conf).delete(outputPath, true);
		if(conf.getBoolean("filtering", false)){
			String ret = "Filter(" + job.getJobName() + ")\n"
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
}
