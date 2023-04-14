//Elena Volpi
//CPSC 4330
//Mapper1 for homework 2
//February 4, 2020

package stubs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper gets all the the users who gave products a 4 star rating or higher 
// We output the key as the product code and value is the user code
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if(!(key.get() == 0)){
			String line = value.toString();
			String[] parseLine = line.split("\t");
			if(parseLine.length > 7){
				Integer starReview = Integer.parseInt(parseLine[7]);
			
			//We want the users who gave products a 4 star rating or higher
				if(starReview >= 4) {
					//parseline[1] is customer, parseline[3] is product
					context.write(new Text(parseLine[3]),new Text(parseLine[1]));
				}
			}
			
		}
		
		
	}
	
}
