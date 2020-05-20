import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;
import java.lang.*;


public class SimpleRandomSampling{
	public static class samplingMapper extends Mapper<Object,Text,NullWritable,Text>{
		private Random rands = new Random();
		private Double percentage;

		protected void setup(Context context) throws IOException,InterruptedException{
			String strPercentage = context.getConfiguration().get("filter_percentage");
			percentage = Double.parseDouble(strPercentage) /100.0;
		}

		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{

			if(rands.nextDouble() < percentage){
				context.write(NullWritable.get(),value);
			}
		}
	}



	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if(args.length != 2){
			System.err.println("Usage SimpleRandomSampling <in> <out>");
			System.exit(2);
		}

		conf.set("filter_percentage","59");
		Job job = new Job(conf,"SimpleRandomSampling");

		// set jar
		job.setJarByClass(SimpleRandomSampling.class);

		// set mapper
		job.setMapperClass(samplingMapper.class);

		// set reducer tasks 0
		job.setNumReduceTasks(0);

		// set outputclasses
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// set input output path
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		// wait for completion
		System.exit(job.waitForCompletion(true)?0:1);
	}
}