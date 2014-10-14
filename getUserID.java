
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class GetUserID {
	
	public static String zipcode;
	public static class Map 
	extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key 
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
		       String line = value.toString();
		       StringTokenizer itr = new StringTokenizer(line,"::"); // line to string token
		       //while (tokenizer.hasMoreTokens()) {
		       int count = 0;
				String[] aryStrings=new String[5];
		       while (itr.hasMoreTokens()) 
				{
		    	  
				  aryStrings[count] = itr.nextToken();
				  count += 1;
		        }
		       	 if(aryStrings[4].equals(zipcode)){
				  String val3 = aryStrings[0].trim();
				  word.set(val3);

			//while (itr.hasMoreTokens()) {
				//word.set(itr.nextToken()); // set word as each input keyword
				context.write(word, one); // create a pair <keyword, 1> 
			//}
		}
	}
	}

	public static class Reduce
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0; // initialize the sum for each keyword
			for (IntWritable val : values) {
				sum += val.get(); 
			}
			//result.set(sum);
			result=null;
			context.write(key, result); // create a pair <keyword, number of occurences>
		}
	}
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: GetUserID <in> <out> <zipcode>");
			System.exit(2);
		}

		zipcode=otherArgs[2];
		// create a job with name "wordcount"
		Job job = new Job(conf, "GetUserID");
		job.setJarByClass(GetUserID.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
//		uncomment the following line to add the Combiner
//		job.setCombinerClass(Reduce.class);
		
		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
