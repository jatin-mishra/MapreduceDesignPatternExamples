import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;
import java.lang.*;

public class distinctRecords{

	public static class recordMapper extends Mapper<Object,Text,Text,NullWritable>{
		private Text data = new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] line = value.toString().split(",");
			data.set(line[1]);
			context.write(data, NullWritable.get());
		}
	}

	public static class recordReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException,InterruptedException{
			context.write(key,NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: TopTenRecord <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"distinctRecords");

		job.setJarByClass(distinctRecords.class);
		job.setMapperClass(recordMapper.class);
		job.setCombinerClass(recordReducer.class);
		job.setReducerClass(recordReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}