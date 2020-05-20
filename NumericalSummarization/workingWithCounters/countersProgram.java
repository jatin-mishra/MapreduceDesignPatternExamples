import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eftimoff.mapreduce.utils.MRDPUtils;

public class CountNumUsersByState{

	public static class CountNumUsersByStateMapper extends Mapper<Object,Text,NullWritable,NullWritable>{
		public static final String STATE_COUNTER_GROUP = "State";
		public static final String UNKNOWN_COUNTER = "Unknown";
		public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";

		private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR",
				"CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
				"IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
				"MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
				"OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
				"VT", "VA", "WA", "WV", "WI", "WY" };

		private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));

		public void map(Object key,Text value, Context context) throws IOException,InterruptedException{
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String location = parsed.get("Location");
			if(location != null && !location.isEmpty()){

				String[] tokens = location.toUpperCase().split("\\s");
				boolean unknown = true;
				for(String state:tokens){
					if(states.contains(state)){
						context.getCounter(STATE_COUNTER_GROUP,state).increment(1);
						unknown  = false;
						break;
					}
				}
				if(unknown){
					context.getCounter(STATE_COUNTER_GROUP,UNKNOWN_COUNTER).increment(1);
				}

			}else{
				context.getCounter(STATE_COUNTER_GROUP,NULL_OR_EMPTY_COUNTER).increment(1);
			}

		}
	}

	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: CountNumUsersByState <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"counters");

		// set jar
		job.setJarByClass(CountNumUsersByState.class);

		// set mapper
		job.setMapperClass(CountNumUsersByStateMapper.class);

		// set numcount zero
		job.setNumReducerTasks(0);

		// set output format
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// set input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean success = job.waitForCompletion(true);

		if(success){
			for(Counter counter: job.getCounters().getGroup(CountNumUsersByStateMapper.STATE_COUNTER_GROUP)){
				System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
			}
		}

		FileSystem.get(conf).delete(new Path(args[1]));
		return sucess?0:1;
	}
}