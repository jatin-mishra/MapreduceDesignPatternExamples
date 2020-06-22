import java.util.zip.*;
import java.io.*;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;




public class bloomTraining{

	public static int getOptimalBloomFilterSize(int numElements, double falsePosRate) {
		return (int) (-(numElements * (double) Math.log(falsePosRate)) / Math.pow(Math.log(2), 2));
	}


	public static int getOptimalK(double numElements, double vectorSize) {
		System.out.println("m : " + vectorSize + " log2: " + Math.log(2) + " n: " + numElements);
		System.out.println((double)vectorSize * Math.log(2));
		return (int) Math.round(((double)vectorSize * Math.log(2)) / (double)numElements);
	}

	public static void main(String[] args) throws Exception{
		if(args.length != 4){
			System.out.println("Usage: bloomTraining <in> <out> <pvalue>[false Positivity] <sizeofSet> ");
			System.exit(0);
		}

		Path inputfile = new Path(args[0]);
		Path output = new Path(args[1]);

		double pval = Double.parseDouble(args[2]);
		int nElements = Integer.parseInt(args[3]);

		int vectorSize = getOptimalBloomFilterSize(nElements, pval);
		int optimalK = getOptimalK(nElements,vectorSize);
		System.out.println(vectorSize + " : " + optimalK);

		BloomFilter filter = new BloomFilter(vectorSize,optimalK,Hash.MURMUR_HASH);

		FileSystem fs = FileSystem.get(new Configuration());
		String line = "";
		for(FileStatus status : fs.listStatus(inputfile)){
			BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(status.getPath()))));

			while((line = br.readLine())!=null){
				filter.add(new Key(line.toString().split(",")[3].getBytes()));
			}
		}

		FSDataOutputStream strm = fs.create(output);
		filter.write(strm);
		strm.flush();
		strm.close();

		System.exit(0);
	}
}