




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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;



import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import java.io.File;





public class makeXML{


	public static class PostMapper extends Mapper<Object,Text,Text,Text>{

		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			outkey.set(parsed.get("id"));
			outvalue.set("P" + value.toString());
		}
	}	

	public static class CommentMapper extends Mapper<Object,Text,Text,Text>{
		private Text outkey = new Text();
		private Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			outkey.set(parsed.get("PostId"));
			outValue.set("C" + value.toString());
			context.write(outkey, outValue);
		}
	}


	public static class UserJoinReducer extends Reducer<Text,Text,Text,NullWritable>{
		private ArrayList<String> comments = new ArrayList<String>();
		private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		private String post = null;

		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{


			post = null;
			comments.clear();


			for(Text t:values){


				if(t.charAt(0) == 'P'){
					post = t.toString().substring(1,toString().length()).trim();
				}else{
					comments.add(t.toString().substring(1,t.toString().length()).trim());
				}

			}


			if(post != null){
				String postWithCommentChildren = nestElements(post,comments);

				context.write(new Text(postWithCommentChildren),NullWritable.get());
			}
		}

		private String nextElements(String post,List<String> comments){


			DocumentBuilder bldr = dbf.newDocumentBuilder();
			Document doc = bldr.newDocument();

			Element postEl = getXmlElementFromString(post);
			Element toAddPostEl = doc.createElement("post");

			copyAttributesToElement(postEl.getAttributes(),toAddPostEl);

			for(String commentXml : comments ){
				Element commentEl = getXmlElementFromString(commentXml);
				Element toAddCommentEl = doc.createElement("comments");
				copyAttributesToElement(commentEl.getAttributes(),toAddCommentEl);
				toAddPostEl.appendChild(toAddCommentEl);
			}

			doc.appendChild(toAddPostEl);
			return transformDocumentToString(toAddPostEl);
		}

		private Element getXmlElementFromString(String xml){
			DocumentBuilder bldr = dbf.newDocumentBuilder();
			return bldr.parse(new InputSource(new StringReader(xml))).getDocumentElement();
		}


		private void copyAttributesToElement(NamedNodeMap attributes,Element element){
			for(int i=0;i<attributes.getLength();++i){
				Attr toCopy = (Attr) attributes.item(i);
				element.setAttribute(toCopy.getName(),toCopy.getValue());
			}
		}


		private String transformDocumentToString(Document doc){
			TransformFactory transFac = TransformFactory.newInstance();
			Transformer transformer = transFac.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"yes");
			StringWriter writer = new StringWriter();
			transformer.transform(new DOMSource(doc), new StreamResult(writer));

			return writer.getBuffer().toString().replaceAll("\n|\r","");
		}

	}



	public static void main(String[] args) throws Exception{

		if(args.length != 3){
			// throw error
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"PostCommentHeirarchy");
		job.setJarByClass(PostCommentHeirarchy.class);

		MultipleInputs.addInputPath(job,new Path(args[0]) , TextInputFormat.class, PostMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]) , TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(UserJoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:2);
	}

}