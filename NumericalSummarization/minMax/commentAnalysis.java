import java.text.*;
import java.util.*;
import java.io.*;

// import GenericOptionsParser
import org.apache.hadoop.util.GenericOptionsParser;


// import Configuration
import org.apache.hadoop.conf.Configuration;

// import job
import org.apache.hadoop.mapreduce.Job;

// import mapper and reduce
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// import input classes
// import org.apache.hadoop.io.Writable;
// import org.apache.hadoop.io.Text;
// import java.io.Exception;

import org.apache.hadoop.io.*;
// import Path
import org.apache.hadoop.fs.Path;

// import FileFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class MinMaxCoupleTuple implements Writable{

	private Date min = new Date();
	private Date max = new Date();
	private long count = 0;
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");

	public Date getMin(){
		return min;
	}

	public void setMin(Date min){
		this.min = min;
	}

	public void setMax(Date max){
		this.max = max;
	}

	public void setCount(long count){
		this.count = count;
	}

	public Date getMax(){
		return this.max;
	}

	public long getCount(){
		return this.count;
	}

	public void readFields(DataInput in) throws IOException{

		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();

	}

	public void write(DataOutput out) throws IOException{
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}

	public String toString(){
		return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	}
}


public class commentAnalysis{


	public static class commentMapper extends Mapper<Object,Text,Text,MinMaxCoupleTuple>{
		private Text id = new Text();
		private MinMaxCoupleTuple data = new MinMaxCoupleTuple();
		private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		private Date minDate = null;
		private Date maxDate = null;
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			// get the line
			String line = value.toString();

			// split the line
			String[] dataAndId = line.split(",");

			// create dates taken from line as string
			try{
				minDate = format.parse(dataAndId[0]);
				maxDate = format.parse(dataAndId[0]);
			}catch(ParseException e){
				System.out.println("error occured while parsing date");
				e.printStackTrace();
			}
			// get Id from line
			String Id = dataAndId[1];

			// set id
			id.set(Id);

			// set min Date
			data.setMin(minDate);

			// setMAxDate
			data.setMax(maxDate);

			// count the comments
			data.setCount(1);

			// write to context
			context.write(id,data);
		}
	}


	public static class commentReducer extends Reducer<Text,MinMaxCoupleTuple,Text,MinMaxCoupleTuple>{
		
		private MinMaxCoupleTuple finalResult = new MinMaxCoupleTuple();
		public void reduce(Text key,Iterable<MinMaxCoupleTuple> values,Context context) throws IOException,InterruptedException{
			finalResult.setMin(null);
			finalResult.setMax(null);
			finalResult.setCount(0);
			int sum=0;


			for(MinMaxCoupleTuple value : values){

				if((finalResult.getMin() == null) || (value.getMin().compareTo(finalResult.getMin())<0)){
					finalResult.setMin(value.getMin());
				}

				if((finalResult.getMax() == null) || (value.getMax().compareTo(finalResult.getMax())>0)){
					finalResult.setMax(value.getMax());
				}

				sum += value.getCount();
			}

			finalResult.setCount(sum);
			context.write(key,finalResult);
		}
	}


	public static void main(String[] args) throws Exception{
		// creating configuration
		Configuration conf = new Configuration();

		// get arguments , to use GenericOptionsParser i have to to download commons-cli.1.2.jar and add to classpath
		// String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		// checking for arguments
		if(args.length != 2){
			System.err.println("Usage: commentAnalysis <in> <out>");
			System.exit(2);
		}

		// creating job
		Job job = new Job(conf,"analysis of comments");


		// set jar class
		job.setJarByClass(commentAnalysis.class);

		// set mapper class
		job.setMapperClass(commentMapper.class);

		// set combiner class
		job.setCombinerClass(commentReducer.class);

		// set reducer class
		job.setReducerClass(commentReducer.class);

		// set outputkey type class
		job.setOutputKeyClass(Text.class);


		// set output value type class
		job.setOutputValueClass(MinMaxCoupleTuple.class);

		// set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// set output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		// call for wait and then exit
		System.exit(job.waitForCompletion(true)?0:1);
	}
}