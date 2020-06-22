//  for each comment get all users

import java.io.*;
import java.util.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


import org.xml.sax.InputSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





class PairOfWritables<L extends Writable, R extends Writable> implements Writable {

	private L leftElement;
	private R rightElement;

	public PairOfWritables() {}


	public PairOfWritables(L left, R right) {
		leftElement = left;
		rightElement = right;
	}


	@Override @SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		String keyClassName = in.readUTF();
		String valueClassName = in.readUTF();

		try {
			Class keyClass = Class.forName(keyClassName);
			leftElement = (L) keyClass.newInstance();
			Class valueClass = Class.forName(valueClassName);
			rightElement = (R) valueClass.newInstance();

			leftElement.readFields(in);
			rightElement.readFields(in);
		} catch (Exception e) {
			throw new RuntimeException("Unable to create PairOfWritables!");
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(leftElement.getClass().getCanonicalName());
		out.writeUTF(rightElement.getClass().getCanonicalName());

		leftElement.write(out);
		rightElement.write(out);
	}


	public L getLeftElement() {
		return leftElement;
	}


	public R getRightElement() {
		return rightElement;
	}

	public L getKey() {
		return leftElement;
	}

	public R getValue() {
		return rightElement;
	}

	public void set(L left, R right) {
		leftElement = left;
		rightElement = right;
	}

	public String toString() {
		return "(" + leftElement + ", " + rightElement + ")";
	}
}

public class workWithXml{


	public static class PostMapper extends Mapper<Object, Text, Text, PairOfWritables>{

		private Map<String,String> mpData = null;

		private Map<String,String> parseXmlPostString(String line) throws Exception{
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(new InputSource(new StringReader(line)));
			doc.getDocumentElement().normalize();

			Map<String, String> mp = new HashMap<String,String>();
			mp.put("Id",doc.getDocumentElement().getAttribute("Id"));
			mp.put("postdata", doc.getDocumentElement().getTextContent());
			return mp;
		}

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
				mpData = parseXmlPostString(value.toString());
			}catch(Exception e){
				System.err.println("Conn't parse postMapper xml");
				System.exit(0);
			}

			// System.out.println(mpData.get("Id") + " : " + "P" + mpData.get("Id") + " : " + mpData.get("postdata"));
			context.write(new Text(mpData.get("Id")),new PairOfWritables(new Text("P" + mpData.get("Id")),new Text(mpData.get("postdata"))));
			
		}

	}


	public static class CommentMapper extends Mapper<Object, Text, Text, PairOfWritables>{

		private Map<String,String > commentMap = null;
		private Map<String,String> xmlToCommentData(String line) throws Exception{


			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(new InputSource(new StringReader(line)));

			Map<String, String> mp = new HashMap<String,String>();
			NodeList nodelist = doc.getElementsByTagName("User");

			Element user =	(Element)nodelist.item(0);
			mp.put("Id",user.getAttribute("Id"));


			Element postId = (Element) user.getElementsByTagName("PostId").item(0);
			mp.put("postId",postId.getTextContent());

			Element comment = (Element) user.getElementsByTagName("Comment").item(0);
			mp.put("comment",comment.getTextContent());

			Element date = (Element) user.getElementsByTagName("Date").item(0);
			mp.put("date",date.getTextContent());

			return mp;
		}


		public  void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				commentMap = xmlToCommentData(value.toString());
			}catch(Exception e){
				System.out.println("error while parsing comment data ");
				System.exit(0);
			}
			context.write(new Text(commentMap.get("postId")), new PairOfWritables(new Text("C" + commentMap.get("Id") + ":" + commentMap.get("date")), new Text(commentMap.get("comment"))));
		}

	}



	public static class PostCommentReducer extends Reducer<Text, PairOfWritables, Text, Text>{


		protected void reduce(Text postId, Iterable<PairOfWritables> commentsOrPosts, Context context) throws IOException , InterruptedException{

			String latestPost = "";
			String commentString = "";

			for(PairOfWritables commentOrPost : commentsOrPosts ){

				if(commentOrPost.getLeftElement().toString().charAt(0) == 'C'){
					String userId = commentOrPost.getLeftElement().toString().substring(1,commentOrPost.getLeftElement().toString().length()-1);
					String comment = commentOrPost.getRightElement().toString();

					commentString += ("(" + userId + "##---> " +  comment + ")");
				}else if(commentOrPost.getLeftElement().toString().charAt(0) == 'P'){

					latestPost = commentOrPost.getRightElement().toString();

				}

			}

			System.out.println(latestPost);

			if(latestPost != ""){
				latestPost = (postId.toString() + "->" + latestPost);
			}else if(postId.toString().length() != 0){
				latestPost = postId.toString();
			}else{
				latestPost = "UNKNOWN";
			}


			context.write(new Text(latestPost), new Text(commentString));
		}





	}


	public static void main(String[] args) throws Exception{


		if(args.length != 3){
			System.err.println("Usage: workWithXml <inPostData> <inCommentData> <outpath> ");
			System.exit(0);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"postAndComment");


		job.setJarByClass(workWithXml.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PostMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(PostCommentReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairOfWritables.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		System.exit(job.waitForCompletion(true)?0:1);
	} 
}