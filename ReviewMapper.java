package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		
		if(key.get() == 0){
			return;
		} else {
			String[] parseLine = line.split("\t");
			Integer starReview = Integer.parseInt(parseLine[7]); 
			context.write(new Text(parseLine[3]),new IntWritable(starReview));
		}
	}
}
