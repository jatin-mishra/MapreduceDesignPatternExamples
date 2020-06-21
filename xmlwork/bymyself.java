import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;

public class bymyself{


	public static void main(String[] args) throws Exception{



		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new File("/home/hduser/bigdata/hadoop/Mapreduce/mapreducePrograms/dataorganization/filename.xml"));

		doc.getDocumentElement().normalize();

		System.out.println(doc.getDocumentElement().getNodeName());


		NodeList lst = doc.getElementsByTagName("student");

		for(int i = 0; i < lst.getLength(); i++){

			Node node = lst.item(i);

			if(node.getNodeType() == Node.ELEMENT_NODE){

				Element element = (Element) node;

				System.out.println(element.getNodeName());

				System.out.println(element.getAttribute("rollno"));
				System.out.println(element.getElementsByTagName("firstname").item(0).getTextContent());
				System.out.println(element.getElementsByTagName("lastname").item(0).getTextContent());
				System.out.println(element.getElementsByTagName("nickname").item(0).getTextContent());
				System.out.println(element.getElementsByTagName("marks").item(0).getTextContent());






			}





		}

	}


}