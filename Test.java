/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import javax.xml.bind.SchemaOutputResolver;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 *
 * @author User
 */
public class Test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)  {
        try{
            String query="CREATE KEYSPACE  [ IF NOT EXISTS ] sakib WITH REPLICATION "
                    + "= {'class':'SimpleStrategy', replication_factor':2};";

            //Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
            Cluster cluster = Cluster.builder().addContactPoint("localhost").build();

            Session session = cluster.connect("sakib");
            //Session session = cluster.connect();

            // session.execute(query);
            session.execute("use sakib;");

            //session.execute("USE fuad");
            for(int j=1; j<=2; j++) {

                System.out.println("Keyspace Created");
                String S = "Comments-0"+String.valueOf(j)+".xml";
                File xmlFile = new File(S);
               // System.out.println("Keyspace Created");
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = (Document) dBuilder.parse(xmlFile);
                doc.getDocumentElement().normalize();
                NodeList nList = doc.getElementsByTagName("row");
               // System.out.println("Keyspace Created");//
                int a = 0;
                for (int i = 0; i < nList.getLength(); i++) {

                   // System.out.println("hi");
                    Node p = nList.item(i);
                    if (p.getNodeType() == Node.ELEMENT_NODE) {
                        Element yy = (Element) p;
                        String s1 = yy.getAttribute("Id");
                        System.out.println(s1);
                        String s2 = yy.getAttribute("PostId");
                        System.out.println(s2);
                        String s3 = yy.getAttribute("Score");
                        System.out.println(s3);
                        String s4 = yy.getAttribute("Text");

                        s4 = s4.replaceAll("-", "");
                        s4 = s4.replaceAll("'", "");
                        s4 = s4.replaceAll("!", "");
                        System.out.println(s4);
                        String s5 = yy.getAttribute("CreationDate");
                        System.out.println(s5);
                        s5=s5.substring(0,10);
                        System.out.println(s5);
                        String s6="";
                        s6=yy.getAttribute("UserId");

                        //session.execute("insert into data(Id, PostId,Score, text) values('"+s1+"','"+s2+"','"+s3+"',\""+s4+"\"));");
                        // session.execute("insert into data3(Id) values('"+s10+"');");

                       // System.out.println("bye");
                        session.execute("insert into maindata1(id, postid, text, score, creationdate, UserId) values(" + s1 + "," + s2 + ", '" + s4 + "'," + s3 + ",'" + s5 + "','"+s6+"');");
                        int k = 0;
                       // System.out.println(k++);
                    }
                }
                System.out.println(a);
                System.out.println("process finished ");
                System.out.println(j);
            }
        }catch(Exception e){}
        // TODO code application logic here
    }

}
