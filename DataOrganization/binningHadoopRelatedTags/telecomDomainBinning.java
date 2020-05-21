import java.io.File;
import java.io.IOException;
import java.io.InterruptedException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class telecomDomainBinning{


	public static class telecomMapper extends Mapper<Object,Text,Text,NullWritable>{
		private MultipleOutputs<Text,NullWritable> multipleOutputs = null;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text,NullWritable>(context);
		}

		public void map(Object key,Text value, Context context) throws IOException,InterruptedException{
			String[] field = value.toString().split(",",-1);
			if(field != null && field.length > 40){
				String type = field[2];
				
				if(type.equalsIgnoreCase("PTP")){
					multipleOutputs.write("datatype",value,NullWritable.get(),"ptp");
				}else if(type.equalsIgnoreCase("SYS")){
					multipleOutputs.write("datatype",value,NullWritable.get(),"sys");
				}else if(type.equalsIgnoreCase("ETH")){
					multipleOutputs.write("datatype",value,NullWritable.get(),"eth");
				}
			}
		}

	}

	public static void main(String[] args) throws Exception{

		if(args.length != 2){
			System.out.println("Usage: telecomDomainBinning <in> <out>");
			System.exit(-1);
		}

		FileUtils.deleteDirectory(new File(args[1]));

		Configuration conf = new Configuration();
		Job job = new Job(conf,"binning map-only");

		job.setJarByClass(telecomDomainBinning.class);
		job.setMapperClass(telecomMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.addOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job,"datatype",TextOutputFormat.class,Text.class,NullWritable.class);
		MultipleOutputs.setCounterEnabled(job,true);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
