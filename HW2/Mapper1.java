/*Loveraj Sidhu
 *
 *File: Mapper1
 *
*/

package stubs;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	private int rating;
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{

		//Parse Data
		String data = value.toString();
		
		//Return Prod_ID and Cust_ID as key value pair
		Text prod_ID = new Text();
		Text user_ID = new Text();
		
		//Return if first row
		if(key.get() == 0)
			return;
		else{
			String [] row_vals = data.split("\t");
			
			//Check to see all the product types that have a rating higher than 4
			rating = Integer.parseInt(row_vals[7]);
			//Set Columns to get prod_id and user_id
			prod_ID.set(row_vals[3]);
			user_ID.set(row_vals[1]);
			
			if(rating >= 4){
				context.write(prod_ID, user_ID);
			}
		}
	}

}
