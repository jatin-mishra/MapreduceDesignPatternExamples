import java.util.*;
import java.io.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;




public class filteringUsingBloom{

	public static class BloomMapper extends Mapper<Object, Text, Text, NullWritable>{
		private BloomFilter bloom = new BloomFilter();
		protected void setup(Context context) throws IOException , InterruptedException {
			URI[] paths = context.getCacheFiles();

			DataInputStream datainput = new DataInputStream(new FileInputStream(paths[0].getPath()));
			bloom.readFields(datainput);
			datainput.close();
		}

		protected void map(Object lineNumber, Text text, Context context) throws IOException, InterruptedException{
			if(bloom.membershipTest(new Key(text.toString().split(",")[3].getBytes()))){
				context.write(text,NullWritable.get());
			}
		}
	}


	public static void main(String[] args) throws Exception{

		if(args.length != 3){
			System.out.println("Usage: filterUsingBloom <in> <bloomTrainedFilter> <out> ");
			System.exit(0);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"filterUsingBloom");

		try{
			job.addCacheFile(new URI(args[1]));
		}catch(Exception e){
			System.out.println("throwing from distributed cache ");
			e.printStackTrace();
		}

		job.setJarByClass(filteringUsingBloom.class);
		job.setMapperClass(BloomMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true)?0:1);

	}

}