import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import java.lang.Math;
import java.util.*;
import java.io.*;
import java.util.Map;
import java.util.Map.Entry;



class MdAndSd implements Writable{
	private double median;
	private double standardDeviation;

	public MdAndSd(){
		this.median = 0.0;
		this.standardDeviation = 0.0;
	}

	public void setMedian(double median){
		this.median = median;
	}

	public void setSD(double sd){
		this.standardDeviation = sd;
	}

	public double getMedian(){
		return this.median;
	}

	public double getSD(){
		return this.standardDeviation;
	}


	public void readFields(DataInput in) throws IOException{
		this.setMedian(in.readDouble());
		this.setSD(in.readDouble());
	}

	public void write(DataOutput out) throws IOException{
		out.writeDouble(this.getMedian());
		out.writeDouble(this.getSD());
	}

	public String toString(){
		return "( " + this.median + "," + this.standardDeviation + " )";
	}
}



public class memoryConsciousMdSd{
	
	public static class memoryConsciousMapper extends Mapper<Object,Text,Text,SortedMapWritable>{
		private IntWritable commentLength = new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);
		private Text uniqueId = new Text();
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			String[] line = value.toString().split(",");
			uniqueId.set(line[0]);
			commentLength.set(line[1].length());

			SortedMapWritable sortedmp = new SortedMapWritable();
			sortedmp.put(commentLength,ONE);

			context.write(uniqueId,sortedmp);
		}
	}


	public static class memoryConsciousCombiner extends Reducer<Text,SortedMapWritable,Text,SortedMapWritable>{
		public void reduce(Text key,Iterable<SortedMapWritable> values,Context context) throws IOException,InterruptedException{

			SortedMapWritable outValue = new SortedMapWritable();

			for(SortedMapWritable v : values){

				for(@SuppressWarnings("rawtypes") 
				Entry<WritableComparable, Writable> entry : v.entrySet()){
					LongWritable count = (LongWritable)outValue.get(entry.getKey());
					
					if(count != null){
						count.set(count.get() + ((LongWritable) entry.getValue()).get());
					}else{
						outValue.put(entry.getKey(),new LongWritable(((LongWritable) entry.getValue()).get()));
					}
				}
			}
		context.write(key,outValue);

		}

	}

	public static class memoryConsciousReducer extends Reducer<Text,SortedMapWritable,Text,MdAndSd>{
		private MdAndSd result = new MdAndSd();
		private TreeMap<Integer,Long> comment = new TreeMap<Integer,Long>();

		public void reduce(Text key,Iterable<SortedMapWritable> values, Context context) throws IOException,InterruptedException{


			float sum = 0;
			long totalComments = 0;
			comment.clear();


			for(SortedMapWritable v: values){

					for(@SuppressWarnings("rawtypes") 
					Entry<WritableComparable,Writable> entry : v.entrySet() ){

						int length = ((IntWritable) entry.getKey()).get();
						long count = ((LongWritable) entry.getValue()).get();

						totalComments += count;
						sum += length * count;


						long storedCount = comment.get(length);

						if(storedCount == null){
							comment.put(length,count);
						}else{
							comment.put(length, storedCount + count);
						}
					}


			}



			long medianIndex = totalComments/2L;
			long previousComments = 0;
			long comments = 0;
			long prevKey = 0;

			// for(@SuppressWarnings("rawtypes") 
				for(Entry<Integer,Long> entry : comment.entrySet()){
				
				comments = previousComments + entry.getValue();

				if(previousComments <= medianIndex && medianIndex < comments){
					if(totalComments%2 == 0  && previousComments == medianIndex){
						result.setMedian((double)(entry.getKey() + prevKey) / (double)2.0);
					}else{
						result.setMedian(entry.getKey());
					}
				}

				previousComments = comments;
				prevKey = entry.getKey();
			}

			double mean = (double)sum/totalComments;

			double sumOfSquares = (double)0.0;
			// for(@SuppressWarnings("rawtypes") 
				for(Entry<Integer,Long> entry : comment.entrySet()){
				sumOfSquares += (entry.getKey() - mean)*(entry.getKey() - mean) * entry.getValue();
			}

			result.setSD((double) Math.sqrt(sumOfSquares / (totalComments - 1)));
			context.write(key,result);

		}
	}

	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: memoryConsciousReducer <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"memoryConscious Median");

		// set jar
		job.setJarByClass(memoryConsciousMdSd.class);

		// set mapper
		job.setMapperClass(memoryConsciousMapper.class);

		// set combiner
		job.setCombinerClass(memoryConsciousReducer.class);

		// set MapOutput Key
		job.setMapOutputKeyClass(Text.class);

		// set MapOutputValue
		job.setMapOutputValueClass(SortedMapWritable.class);

		// set reducer
		job.setReducerClass(memoryConsciousReducer.class);

		// set outputKey
		job.setOutputKeyClass(Text.class);

		// set outputValue
		job.setOutputValueClass(MdAndSd.class);

		// set add inputpath
		FileInputFormat.addInputPath(job,new Path(args[0]));

		// set add output path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		// set Num reducers
		job.setNumReduceTasks(2);


		// waitfor completion
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
