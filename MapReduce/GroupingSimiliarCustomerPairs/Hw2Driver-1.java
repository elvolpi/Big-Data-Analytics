//Elena Volpi
//Hw2 driver, hw2 
//CPSC 4330

package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

public class Hw2Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
		      System.out.printf("Usage: Hw2 <input dir>  <temp dir > <output dir>\n");
		      System.exit(-1);
		}
		Configuration conf = new Configuration();
		
		Job job1 = Job.getInstance(conf,"job1");
		job1.setJarByClass(Hw2Driver.class);
		job1.setJobName("Hw2Driver");
		
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(UserPairWritable.class);
		
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1,new Path(args[1]));
	    
	    
		
	    
	    int flag = job1.waitForCompletion(true)?0:1;
	    if(flag != 0) {
	    	System.out.println("Job1 failed, exiting");
	    	System.exit(flag);
	    }
	    
	    
	    Job job2 = Job.getInstance(conf,"job2");
		job2.setJarByClass(Hw2Driver.class);
		job2.setJobName("Hw2Driver");
		
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		
		job2.setMapOutputKeyClass(UserPairWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(UserPairWritable.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2,new Path(args[2]));
	    
	    
	    
		boolean success = job2.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
