/*Loveraj Sidhu
 *
 *File: Driver
 *
*/

package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Hw2Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception{
		// TODO Auto-generated method stub

		//Validate that the two arguments were passed from the command line
		if(args.length != 2){
			System.out.printf("Usage: " + this.getClass().getName() + " <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(Hw2Driver.class);

		job.setJobName("Amazon User Pairs");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(UserPairWritable.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		
		// Job2 not complete :( 
		/*
		Job job2 = new Job(getConf());
		job2.setJarByClass(Hw2Driver.class);
		job2.setMapperClass(Mapper2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		*/
		
		return success ? 0 : 1;
	}
	
	public static void main(String [] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new Hw2Driver(), args);
		System.exit(exitCode);
	}

}
