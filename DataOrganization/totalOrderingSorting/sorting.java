import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;



public class sorting{


	public static class lastAccessDateMapper extends Mapper<Object,Text,Text,Text>{
		private Text outkey = new Text();

		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			Map<String,String> = MRDPUtils.transformXmlToMap(value.toString());
			outkey.set(parsed.get("lastAccessDate"));
			context.write(outkey, value);
		}
	}


	public static class orderMapper extends Mapper<Object,Text,Text,Text>{

		public void map(Object key,Text value Context context) throws IOException,InterruptedException{
			context.write(key,value);
		}
	}


	public static class orderReducer extends Reducer<Text,Text,Text, NullWritable>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			for(Text t : values){
				context.write(t, NullWritable.get());
			}
		}
	}



	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			// throw errors
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"totalOrderSorting");

		job.setJarByClass(sorting.class);

		job.setMapperClass(lastAccessDateMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job,new Path(args[0]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job,new Path(args[1] + "_staging"));

		int code = job.waitForCompletion(true)?0:1;

		if(code == 0){
			Job orderJob = new Job(conf,"orderingwork");

			orderJob.setJarByClass(sorting.class);

			orderJob.setMapperClass(orderMapper.class);
			orderJob.setReducerClass(orderReducer.class);

			orderJob.setNumReducerTasks(10);

			orderJob.setPartitionerClass(totalOrderPartitioner.class);
			totalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),new Path(args[1] + "_partitions.lst"));

			orderJob.setOutputKeyClass(Text.class);
			orderJob.setOutputValueClass(Text.class);

			orderJob.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(orderJob,new Path(args[1]+ "_staging"));

			TextOutputFormat.setOutputPath(orderJob,new Path(args[2]));


			orderJob.getConfiguration().set("mapred.textoutputformat.separator","");

			InputSampler.writePartitionFile(orderJob,new InputSampler.RandomSampler(0.001,10000));

			code = orderJob.waitForCompletion(true)?0:2;
		}

		FileSystem.get(new Configuration()).delete(new Path(args[1] + "_partitions.lst"));
		FileSystem.get(new Configuration()).delete(new Path(args[1] + "_staging"));

		System.exit(code);
	}
}