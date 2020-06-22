import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;

public class getSize{

	public static class usingCounter extends Mapper<Object, Text, NullWritable, NullWritable>{

		public static final String GROUP_NAME = "countAll";
		public static final String ONLY_GROUP = "records";

		protected void map(Object key, Text record , Context context) throws IOException, InterruptedException{
			context.getCounter(GROUP_NAME,ONLY_GROUP).increment(1);
		}

	}


	public static void main(String[] args) throws Exception{

		if(args.length != 2){
			System.out.println("Usage: getSize <in> <out>");
			System.exit(0);
		}
	
		Configuration conf = new Configuration();
		Job job = new Job(conf, "getSizeforBloom");
		job.setNumReduceTasks(0);


		job.setJarByClass(getSize.class);
		job.setMapperClass(usingCounter.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int code = job.waitForCompletion(true)?0:1;

		if(code == 0){
			for(Counter counter : job.getCounters().getGroup(usingCounter.GROUP_NAME)){
				System.out.println(counter.getDisplayName() + " : " + counter.getValue() );
			}
		}

		System.exit(code);

	}














}