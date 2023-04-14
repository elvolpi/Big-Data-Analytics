package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReviewReducer extends Reducer<Text, IntWritable, Text, Text> {
	 @Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
		 long sum = 0;
		 long count = 0; 
		 for(IntWritable value: values){
			 sum += value.get();
			 count++;
		 }
		 if(count != 0 ){
			 double avg = (double)sum/(double)count;
			 String str = Double.toString(avg)+ "\t" + Double.toString(count);
			 context.write(key, new Text(str));
		 }
		 
	 }
}
