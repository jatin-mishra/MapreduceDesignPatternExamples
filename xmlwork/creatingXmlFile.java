


import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import java.io.FileOutputStream;

import java.util.Random;


public class creatingXmlFile{


	public static void main(String[] args) throws Exception{
	
	try{





		Random random = new Random();

		for(int i =0 ;i<1000;i++){

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();

		// root element
		Element rootElement = doc.createElement("cars");
		doc.appendChild(rootElement);


		//super cars element
			Element supercars = doc.createElement("superCars");
			rootElement.appendChild(supercars);


			Attr attr = doc.createAttribute("company");
			attr.setValue("Ferrari" + Double.toString(random.nextDouble()));
			supercars.setAttributeNode(attr);


			// carname element
			Element carname = doc.createElement("carname");
			Attr attrType = doc.createAttribute("type");
			attrType.setValue("ferrari One" + Double.toString(random.nextDouble()));
			carname.setAttributeNode(attrType);

			carname.appendChild(doc.createTextNode("ferrari 101" + Double.toString(random.nextDouble())));
			supercars.appendChild(carname);

			Element elementname = doc.createElement("carname2");
			Attr atr2 = doc.createAttribute("type");
			atr2.setValue("ferrari Two"+ Double.toString(random.nextDouble()));
			elementname.setAttributeNode(atr2);

			elementname.appendChild(doc.createTextNode("ferrari 202" + Double.toString(random.nextDouble())));
			supercars.appendChild(elementname);

		

		// write the content to the xml file
		TransformerFactory factor = TransformerFactory.newInstance();
		Transformer transformer = factor.newTransformer();
		

		FileOutputStream fs = new FileOutputStream("/home/hduser/bigdata/hadoop/Mapreduce/mapreducePrograms/dataorganization/other.xml",true);
		
		fs.write("\n".getBytes());
		fs.flush();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(fs);
		transformer.transform(source, result);

		StreamResult res = new StreamResult(System.out);
		transformer.transform(source, res);

	}

	}catch(Exception e){
		e.printStackTrace();
	}


	}
	
}