/*
 * Loveraj Sidhu
 * Reducer file
 * Hw1 
 */

package AmazonRatingMapReduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AmazonReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
			
		long sumRate = 0, countReviews = 0;
		
			for(DoubleWritable value : values){
				sumRate += value.get();
				countReviews++;
			} 
			
			double result = (double)sumRate /(double)countReviews;
			String temp = "Average rating is: " + Double.toString(result) + 
					" Total Reviews is: " + Double.toString(countReviews);
			context.write(key, new Text(temp));
		
	}

}
