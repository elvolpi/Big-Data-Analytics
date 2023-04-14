//Elena Volpi
//Reducer 2, homework 2
//CPSC 4330

package stubs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer2 extends Reducer<UserPairWritable, Text, UserPairWritable, Text> {
	@Override
	public void reduce(UserPairWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		//We want pairs of users with three common products 
		int counter = 0;
		String output  = "";
		for(Text value: values){
			output += value.toString() + ",";
			counter++; 
		}
		if(counter >=3){
			//Throw out the last comma. 
			output = output.substring(0,output.length()-1);
			context.write(key, new Text(output));
		} else {
			return;
		}
		
		
	}
}
