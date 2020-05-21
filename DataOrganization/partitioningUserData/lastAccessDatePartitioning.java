import java.io.*;
import java.net.URI;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// also import MRDPUtils

public class lastAccessDatePartitioning{

	public static class lastAccessDateMapper extends Mapper<Object,Text,IntWritable,Text>{
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		private IntWritable outkey = new IntWritable();

		protected void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String strDate = parsed.get("LastAccessDate");

			Calendar cal = Calendar.getInstance();
			cal.setTime(frmt.parse(strDate));
			outkey.set(cal.get(Calendar.YEAR));

			context.write(outkey, value);
		}
	}



	public static class lastAccessDatePartitioner extends Partitioner<IntWritable,Text> implements Configuration{

		private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
		private Configuration conf = null;
		private int minLastAccessDateYear = 0;

		public int getPartition(IntWritable key, Text value, int numPartitions){
			return key.get() - minLastAccessDateYear;
		}

		public Configuration getConf(){
			return conf;
		}

		public void setConf(Configuration conf){
			this.conf = conf;
			minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
		}

		public static void setMinLastAccessDate(Job job,int minLastAccessDateYear){
			job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,minLastAccessDateYear);
		}
	}	


	public static class lastAccessDateReducer extends Redcuer<InteWritable,Text,Text,NullWritable>{
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			for(Text t: values){
				context.write(t,NullWritable.get());
			}
		}
	}



	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			// throw error
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"customPartitioner");

		job.setJarByClass(lastAccessDatePartitioning.class);
		job.setMapperClass(lastAccessDateMapper.class);
		job.setPartitionerClass(lastAccessDatePartitioner.class);
		lastAccessDatePartitioner.setMinLastAccessDate(job,2016);
		job.setNumReducerTasks(4);
		job.setReducerClass(lastAccessDateReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);


	}
}