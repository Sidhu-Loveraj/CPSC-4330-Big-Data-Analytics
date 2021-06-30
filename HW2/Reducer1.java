/*Loveraj Sidhu
 *
 *File: Reducer
 *
*/



package stubs;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class Reducer1 extends Reducer<Text, Text, UserPairWritable, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context)
		throws IOException, InterruptedException{
		
		//Variables 
		String user1 = new String();
		String user2 = new String();
		
		//Create an tree set to remove duplicate values and sort the order of users
		TreeSet<String> sortedSet = new TreeSet<String>();
		//Convert Tree Set to list to get each combo pairs
		
 		//Add customers into tree set
		for(Text customer : value){
			String user = customer.toString();
			sortedSet.add(user);
		}
		
		//Convert Tree Set to list to get each combo pairs
		List<String> userList = new ArrayList<String>(sortedSet);
		
		//Compare to see if users have the same products
		for(int i = 0; i < userList.size(); i++){
			for(int j = i+1; j < userList.size(); j++){
					user1 = userList.get(i);
					user2 = userList.get(j);
					context.write(new UserPairWritable(user1, user2), new Text(key));
				}
		}
	}
}
