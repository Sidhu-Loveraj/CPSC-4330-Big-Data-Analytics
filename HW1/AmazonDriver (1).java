/*
 * Loveraj Sidhu
 * MDriver file
 * Hw1 
 */

package AmazonRatingMapReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class AmazonDriver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//Validate that two arguments were passed from the command line
		if(args.length != 2){
			System.out.printf("Usage: AmazonDrive <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(AmazonDriver.class);
		
		job.setJobName("Amazon Product Rating");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AmazonMapper.class);
		job.setReducerClass(AmazonReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}

}
