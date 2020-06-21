

import java.io.*;
import java.util.Random;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class createPostData{


	private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";


	public static String randomAlphaNumeric(int count) {
		String line = "";
		while (count-- != 0) {
			line = line + ALPHA_NUMERIC_STRING.charAt((int)(Math.random()*ALPHA_NUMERIC_STRING.length()));
		}
		return line;
	}

	public static void main(String[] args) throws Exception{

		for(int i = 0;i<5000;i++){

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();


		Element post = doc.createElement("Post");
		Attr attr = doc.createAttribute("Id");
		attr.setValue(randomAlphaNumeric(2));
		post.setAttributeNode(attr);
		post.appendChild(doc.createTextNode(randomAlphaNumeric(5) + "<<<-----post-----| "));
		doc.appendChild(post);


		FileOutputStream fout = new FileOutputStream("/home/hduser/bigdata/hadoop/Mapreduce/mapreducePrograms/dataorganization/xmlwork/UserPost.xml",true);
		fout.write("\n".getBytes());
		fout.flush();

		TransformerFactory trasFac = TransformerFactory.newInstance();
		Transformer transformer = trasFac.newTransformer();

		DOMSource dom = new DOMSource(doc);
		StreamResult result = new StreamResult(fout);

		transformer.transform(dom,result);

		StreamResult console = new StreamResult(System.out);
		transformer.transform(dom,console);

	}


	}
}