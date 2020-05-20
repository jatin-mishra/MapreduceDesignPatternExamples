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



public class TopTenRecord{

	public static class recordMapper extends Mapper<Object,Text,NullWritable,Text>{

		private TreeMap<Integer,String> recordMap = new TreeMap<Integer,Text>();

		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			
			String[] data = value.toString().split(",");
			recordMap.put(data[1].length(),data[1]);
			if(recordMap.size() > 10){
				recordMap.remove(recordMap.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException,InterruptedException{
			Text data = null;
			for(String s:recordMap.values()){
				data = new Text();
				data.set(s);
				context.write(NullWritable.get(),data);
			}
		}

	}


	public static class recordReducer extends Reducer<NullWritable,Text,NullWritable,Text>{
		private TreeMap<Integer,String> recordMap = new TreeMap<Integer,String>();
		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			Text data = null;

			for(Text value: values){
				String data = value.get();

				recordMap.put(data.length(),data);
				if(recordMap.size() > 10){
					recordMap.remove(recordMap.firstKey());
				}
			}	
		
			for(String t: recordMap.descendingMap().values()){
				data = new Text();
				data.set(t);
				context.write(NullWritable.get(),data);
			}
		}
	}


	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: TopTenRecord <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"TopTenRecord");

		job.setJarByClass(TopTenRecord.class);
		job.setMapperClass(recordMapper.class);
		job.setReducerClass(recordReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}

