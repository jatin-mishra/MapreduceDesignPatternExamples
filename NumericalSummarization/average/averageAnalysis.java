// calculate average corona cases everymonth per state
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.*;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class averageAnalysis{

	public static class coronaMapper extends Mapper<Object,Text,Text,IntWritable>{
		private IntWritable cases = new IntWritable();
		private Text keyPattern = new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			System.out.println(value);
			String[] line = value.toString().split(",");
			String newKey = line[1] + line[0].substring(5,7);
			keyPattern.set(newKey);
			int count = Integer.parseInt(line[3]);
			cases.set(count);
			context.write(keyPattern,cases);
		}
	}

	public static class coronaReducer extends Reducer<Text,IntWritable,Text,DoubleWritable>{
		private DoubleWritable average = new DoubleWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			double d = 0.0;
			double count = 0;
			System.out.print(key + " : " );
			for(IntWritable x : values){
				count++;
				System.out.print(x.get() + " ");
				d = d + (double)x.get();
			}
			System.out.println();
			average.set(d/count);
			context.write(key,average);
		}
	}

	public static void main(String[] args) throws Exception{

		// create config class
		Configuration conf = new Configuration();

		if(args.length != 2){
			System.err.println("Usage: averageAnalysis <in> <out>");
			return;
		}

		// create job
		Job job = new Job(conf,"average calculator");

		// set jar
		job.setJarByClass(averageAnalysis.class);

		// set mapper
		job.setMapperClass(coronaMapper.class);
		job.setReducerClass(coronaReducer.class);

		// job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// set outputkey format
		job.setOutputKeyClass(Text.class);

		// set outputvalue format
		job.setOutputValueClass(DoubleWritable.class);

		// set reducer

		// set inputpath
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// set output path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));



		System.exit(job.waitForCompletion(true)?0:1);

	}
}