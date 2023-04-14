//Elena Volpi
//Mapper2, homework2

package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;


public class Mapper2 extends Mapper<LongWritable, Text, UserPairWritable, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//deliminate by tabs 
		String line2 = value.toString();
		String[] parseLine2 = line2.split("\t");
		//The first element in parseline is the product key, are users 
		
		ArrayList<UserPairWritable> userPairs = new ArrayList<UserPairWritable>();
		
		//The string compareTo method returns a negative number if temp[0] is less than temp[1] in lexicographic order.
		// If temp[0] is greater than temp[1], returns a positive number
		// In the first job, the case where temp[1] would equal temp[0] is already filtered out. 
		
		//This method is utilized to have all the pairs be (lesser element, greater element) 
		//mapper cut out any duplicates, (u2,u1) is equal to (u1, u2) so we have no need to include both
		//The contains method stops any duplicates being added to userPairs
		for(int i = 1; i < parseLine2.length; i= i+2) {
			//String[] temp = parseLine2[i].split(",");
			if(parseLine2[i].compareTo(parseLine2[i+1]) < 0){
				UserPairWritable pair1 = new UserPairWritable(parseLine2[i], parseLine2[i+1]); 
				if(!userPairs.contains(pair1)) {
					userPairs.add(pair1);
				}
			} else {
				UserPairWritable pair2 = new UserPairWritable(parseLine2[i+1],parseLine2[i]);
				if(!userPairs.contains(pair2)){
					userPairs.add(pair2);
				}
			}
		}
		//key is the pairs, value is the parseLine2 is the product key 
		for(UserPairWritable pair: userPairs){
			context.write(pair, new Text(parseLine2[0]));
		}
		
	}
}
