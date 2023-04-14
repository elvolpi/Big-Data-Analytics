package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class AverageReview {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		      System.out.printf("Usage: AverageReview <input dir> <output dir>\n");
		      System.exit(-1);
		}
		Job job = new Job();
		
		job.setJarByClass(AverageReview.class);
		job.setJarByClass(AverageReview.class);
		job.setJobName("Average Review");
		
		job.setMapperClass(ReviewMapper.class);
		job.setReducerClass(AvgReviewReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
