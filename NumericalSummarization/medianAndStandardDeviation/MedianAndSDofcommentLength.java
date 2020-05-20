import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;

import java.lang.Math;
import java.util.*;
import java.io.*;


class MdAndSd implements Writable{
	private double median;
	private double standardDeviation;

	public MdAndSd(){

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

public class MedianAndSDofcommentLength{

	public static class commentMapper extends Mapper<Object,Text,Text,IntWritable>{
		private Text id = new Text();
		private IntWritable lengthOfComment = new IntWritable();

		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] line = value.toString().split(",");
			id.set(line[0]);
			lengthOfComment.set(line[1].length());
			context.write(id,lengthOfComment);
		}
	}


	public static class commentReducer extends Reducer<Text,IntWritable,Text,MdAndSd>{
		private ArrayList<Integer> list = new ArrayList<Integer>();
		private MdAndSd medianAndStandD = new MdAndSd();

		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
			int sum = 0;
			int count = 0;

			for(IntWritable value : values){
				sum += (value.get());
				list.add(value.get());
				count++;
			}

			double mean = (double)((double)sum/(double)count);

			Collections.sort(list);


			double sumOfSquares = 0.0;

			for(Integer val : list){
				sumOfSquares += (double)((double)val - mean)*((double)val - mean);
			}

			medianAndStandD.setSD((double)Math.sqrt(sumOfSquares / ((double)count-1)));

			if((count % 2 ) == 0 )
				medianAndStandD.setMedian(  (double)(  ((int)list.get(count/2 - 1)) + ((int)list.get(count/2)))/(double)2.0);
			else
				medianAndStandD.setMedian((double)list.get((int)count/2));

			context.write(key,medianAndStandD);

		}
	}


	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: MedianAndSDofcommentLength <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"medianAndStd");

		// set jar
		job.setJarByClass(MedianAndSDofcommentLength.class);

		// set mapperclass
		job.setMapperClass(commentMapper.class);

		// set reducer
		job.setReducerClass(commentReducer.class);


		// set MapOutputKeyClass
		job.setMapOutputKeyClass(Text.class);


		// set MapOutuptValueClass
		job.setMapOutputValueClass(IntWritable.class);


		// set OutputKeyClass
		job.setOutputKeyClass(Text.class);

		// set OutputValue Class
		job.setOutputValueClass(MdAndSd.class);

		// setNumReducers
		job.setNumReduceTasks(2);

		// set FIleInput path
		FileInputFormat.addInputPath(job,new Path(args[0]));

		// set fileoutputpath
		FileOutputFormat.setOutputPath(job,new Path(args[1]));


		System.exit(job.waitForCompletion(true)?0:1);
	}

}