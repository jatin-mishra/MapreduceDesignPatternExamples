import java.util.*;
import java.text.*;
import java.io.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Document;



public class createCommentData{


	private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	
	public static String randomAlphaNumeric(int count) {
		String line = "";
		while (count-- != 0) {
			line = line + ALPHA_NUMERIC_STRING.charAt((int)(Math.random()*ALPHA_NUMERIC_STRING.length()));
		}
		return line;
	}

	public static String randomDate(){
		Random rand = new Random();
			int year = Math.abs(rand.nextInt());
			int month = Math.abs(rand.nextInt());
			int date = Math.abs(rand.nextInt());
		Date dte = new Date((year%30) + 1990, month%12+1, date%31);
		return dte.toString();
	}

	public static void main(String[] args) throws Exception{


		for(int i =0;i<1000;i++){

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();

		Element users = doc.createElement("Users");
		doc.appendChild(users);


		// creating user
		Element user = doc.createElement("User");
		users.appendChild(user);

		// setting ids
		Attr id = doc.createAttribute("Id");
		id.setValue(randomAlphaNumeric(3));
		user.setAttributeNode(id);

		// // post id
		Element postid = doc.createElement("PostId");
		postid.appendChild(doc.createTextNode(randomAlphaNumeric(2)));
		user.appendChild(postid);



		// // creating comment
		// Random randm = new Random();
		
		Element comment = doc.createElement("Comment");
		comment.appendChild(doc.createTextNode(randomAlphaNumeric(20) + " " + randomAlphaNumeric(10)));
		user.appendChild(comment);


		// // addDate
		Element date = doc.createElement("Date");
		date.appendChild(doc.createTextNode(randomDate()));
		user.appendChild(date);


		FileOutputStream fout = new FileOutputStream("/home/hduser/bigdata/hadoop/Mapreduce/mapreducePrograms/dataorganization/UserComment.xml",true);
		// if(i!=0){
			fout.write("\n".getBytes());
			fout.flush();
		// }

		TransformerFactory transFac = TransformerFactory.newInstance();
		Transformer transformer = transFac.newTransformer();

		DOMSource source = new DOMSource(doc);
		StreamResult stream = new StreamResult(fout);
		transformer.transform(source,stream);

		StreamResult console = new StreamResult(System.out);
		transformer.transform(source,console);
	}
	
	}
}