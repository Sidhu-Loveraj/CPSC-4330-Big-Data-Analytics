/*
 * Loveraj Sidhu
 * Mapper file
 * Hw1 
 */

package AmazonRatingMapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AmazonMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
	private DoubleWritable rating = new DoubleWritable();
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	

		//Parse data line by line
		String line = value.toString();
		
		//Return product_id as key and value should
		if(key.get() == 0)
			return;
		else{
			String [] row_vals = data.split("\t");
			rating.set(Double.parseDouble(row_vals[7]));
			context.write(new Text(row_vals[3]), rating);
		}
		
	}

}
