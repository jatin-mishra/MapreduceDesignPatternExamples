import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import com.eftimoff.mapreduce.utils.MRDPUtils;

public class WikipediaExtractor{
	public static class extractingMapper extends Mapper<Object,Text,Text,Text>{
		private Text outKey = new Text();
		Pattern HOSTNAME_MATCHER = Pattern.compile("\\.?wikipedia\\.org/.*",Pattern.CASE_INSENSITIVE);

		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			String body = parsed.get("body");
			String postType = parsed.get("PostTypeId");
			String rowId = parsed.get("Id");

			if(body == null || (postType != null && postType.equals("1"))){
				return;
			}

			body = body.toLowerCase();
			outKey.set(rowId);
			parseAndWriteWikipediaUrls(body, context);
		}

		private void parseAndWriteWikipediaUrls(String body, Context context) throws IOException, InterruptedException {
			Document doc = Jsoup.parse(body);

			for (Element link : doc.select("a")) {
				if (!link.hasAttr("href") || link.attr("href") == null
						|| link.attr("href").length() == 0) {
					continue;
				}
				String url = link.attr("href");
				Matcher matcher = HOSTNAME_MATCHER.matcher(url);
				if (!matcher.find()) {
					continue;
				}
				context.write(new Text(url), outKey);
			}
		}

	}

	public static class WikipediaUrlReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Text id : values) {
				if (first) {
					first = false;
				} else {
					sb.append(" ");
				}
				sb.append(id.toString());
			}
			result.set(sb.toString());
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if(args.length != 2){
			System.err.println("Usage: WikipediaExtractor <in> <out>");
			System.exit(2);
		}

		// set jar
		job.setJarByClass(WikipediaExtractor.class);

		// set mapper
		job.setMapperClass(extractingMapper.class);

		// set reducer
		job.setReducerClass(WikipediaUrlReducer.class);

		// set mapOutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// add input and output path
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		System.exit(job.waitForCompletion(true)?0:1);
	}
}