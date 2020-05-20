import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;



public class bloomFilterWithDistributedCache{

	public static class BloomMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		private BloomFilter filter = new BloomFilter();

		protected void setup(Context context) throws IOException,InterruptedException{
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
			DataInputStream datain = new DataInputStream(new FileInputStream(files[0].getPath()));
			filter.readFields(datain);
			datain.close();
		}


		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString().split(",")[1];
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				String word = tokenizer.nextToken();
				if(filter.membershipTest(new Key(word.getBytes()))){
					context.write(line,NullWritable.get());
					break;
				}
			}
		}
	}

	public Integer getOptimalBloomFilterSize(Long num,Double fpRate){
		return (int)(-num*(double)Math.log(fpRate))/(pow((double)Math.log(2),2));
	}

	public Integer getOptimalK(Long num,Long size){
		return (int)size*(Math.log(2))/num;
	}


	public static trainBloomFilter(Path input,Path output,Long num,Double fpRate){

		int vectorSize = getOptimalBloomFilterSize(num,fpRate);
		int nbHash = getOptimalK(num,vectorSize);


		BloomFilter filter = new BloomFilter(vectorSize,nbHash,Hash.MURMUR_HASH);

		String line = null;
		int numElements = 0;
		FileSystem fs = FileSystem.get(new Configuration());

		for(FileStatus status : fs.listStatus(input)){
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(status.getPath()))));
			while((line = rdr.readLine()) != null){
				filter.add(new Key(line.getBytes()));
				++numElements;
			}

			rdr.close();
		}

		FSDataOutputStream strm = fs.create(output);
		filter.write(strm);
		strm.flush();
		strm.close();
		System.exit(0);
	}


	public static void main(String[] args) throws Exception{
		if(args.length != 3){
			System.err.println("Usage: bloomFilterWithDistributedCache <bloomInputPath> <numMembers> <falsePostiveRate> <in> <out> <bloomOutputPath>");
			System.exit(2);
		}

		Path bloomInput = new Path(args[0]);
		long numMembers = Long.parseLong(args[1]);
		double fprate = Double.parseDouble(args[2]);
		Path input = new Path(args[3]);
		Path output = new Path(args[4]);
		Path bloomOutput = new Path(args[5]);
		URI bloom = new URI(args[5]);

		trainBloomFilter(bloomInput, bloomOutput, numMembers,fprate);


		Configuration conf = new Configuration();
		Job job = new Job(conf,"filtering using BloomFilter");

		job.setJarByClass(bloomFilterWithDistributedCache.class);
		job.addCacheFile(bloom);
		job.setMapperClass(BloomMapper.class);
		job.setNumReduceTasks(0);
		TextInputFormat.setInputPaths(job,input);
		TextOutputFormat.setOutputPath(job,output);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		System.exit(job.waitForCompletion(true)?0:1);
	}


}