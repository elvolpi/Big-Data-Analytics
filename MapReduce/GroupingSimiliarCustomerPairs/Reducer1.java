//Elena Volpi
//CPSC 4330
//Reducer1 for homework 2
//February 4, 2020

package stubs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

//Our key is the product id, the values are the users who gave that product
//a 4 star or better rating. 
// We want all the pairs of users.
public class Reducer1 extends Reducer<Text, Text, Text, UserPairWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		
		//Since I want to be able to go through an array to find all pairs of users,
		//this is all just to get the values into an array. 
		ArrayList<String> arrayTemp = new ArrayList<String>();
		for(Text value: values ) {
				arrayTemp.add(value.toString());
		}
		
		
		//Now we want to get pairs of users
		for(int i = 0; i<arrayTemp.size(); i++) {
			for(int j = i+1; j < arrayTemp.size(); j++){
				//when user[i] is not equal to user[j], 
				if(!arrayTemp.get(i).equals(arrayTemp.get(j))){
					context.write(key, new UserPairWritable(arrayTemp.get(i),arrayTemp.get(j)));
				}
				//key is product id, value is a pair 
			}
		}	
		
	}

}
