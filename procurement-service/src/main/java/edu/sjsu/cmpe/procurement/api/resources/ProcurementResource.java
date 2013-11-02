package edu.sjsu.cmpe.procurement.api.resources;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.domain.Book;
import edu.sjsu.cmpe.procurement.domain.BookOrder;
import edu.sjsu.cmpe.procurement.domain.ShippedBooks;


@Every("10s")
public class ProcurementResource extends Job {
	
	private int numMessages = 0;
	private int[] isbns = new int[50];
	
	
	public int[] getIsbns() {
		return isbns;
	}

	public void addIsbn(int isbn) {
		isbns[numMessages-1]=isbn;
	}
	
	public void removeIsbn(){
		
		for (int i = 0 ; i<50; i++)
		isbns[i]=0;
	}

	public void incrementNumMessages() {
		 numMessages++;
	}
	
	public int getNumMessages(){
		return numMessages;
	}
	
	
	@Override	
	public void doJob() throws Exception{
		
		pullMessageFromQueue();	
		
		ShippedBooks shippedBooks = getDataFromPublisher();
		
		for (int i = 0; i<shippedBooks.getNumBooks();i++){
			String category = shippedBooks.getShipped_books().get(i).getCategory();
			publishBooks(shippedBooks.getShipped_books().get(i),category);
		}
	
	}

	public void pullMessageFromQueue() throws JMSException {
		
	String user = env("APOLLO_USER", "admin");
	String password = env("APOLLO_PASSWORD", "password");
	String host = env("APOLLO_HOST", "54.215.210.214");
	int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
	String queue = "/queue/69676.book.orders";
	String destination = arg(0, queue);
	
	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
	factory.setBrokerURI("tcp://" + host + ":" + port);

	Connection connection = factory.createConnection(user, password);
	connection.start();
	
	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	Destination dest = new StompJmsDestination(destination);
	MessageConsumer consumer = session.createConsumer(dest);

	System.out.println("Waiting for messages from " + queue + "...");
	long waitUntil = 5000; // wait for 10 sec
	Message msg = null;
	String body = null;
	while(true) {
	    //Message msg;
		try {
			msg = consumer.receive(waitUntil);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	    if( msg instanceof  TextMessage ) {
	           //String body;
			try {
				body = ((TextMessage) msg).getText();
			} catch (JMSException e) {
				e.printStackTrace();
			}
	           System.out.println("Received message = " + body);
	           
	           //get ISBN from message and add it to list of ISBNs
	           addIsbn(Integer.parseInt(body.split(":")[1]));
	           
	           incrementNumMessages();
	           
	    } 
	    else if (msg == null) {
	          System.out.println("No new messages. Exiting due to timeout - " + waitUntil / 1000 + " sec");
	          if (getNumMessages()==0){
	        	  System.out.println(getNumMessages() + " messages received");
	        	  sendPostRequest(getIsbns());
	        	  removeIsbn();
	          }
	        	  
	          break;
	    } 
	    else {
	         System.out.println("Unexpected message type: " + msg.getClass());
	    }
	} // end while loop
	try {
		connection.close();
	} catch (JMSException e) {
		e.printStackTrace();
	}
	System.out.println("Done");
	}
	
	
	public void sendPostRequest(int[] isbnList){
		
		BookOrder bookOrder = new BookOrder();
		bookOrder.setId("69676");
		bookOrder.setOrderBookIsbns(isbnList);
		
		Client client = Client.create();
		WebResource webResource = client.resource("http://54.215.210.214:9000/orders");
		ClientResponse response = webResource.type("application/json").post(ClientResponse.class,bookOrder);

		System.out.println(response.getEntity(String.class));	
	
	}
	
	
	public void publishBooks(Book book, String category) throws JMSException{
		
		String user = env("APOLLO_USER", "admin");
		String password = env("APOLLO_PASSWORD", "password");
		String host = env("APOLLO_HOST", "54.215.210.214");
		int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
		String destination = arg(0, "/topic/69676.book."+ category);

		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);

		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		MessageProducer producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			
		TextMessage msg = session.createTextMessage(createMessage(book));
		msg.setLongProperty("id", System.currentTimeMillis());
		producer.send(msg);
		
		System.out.println(msg.toString());
		connection.close();
	}
	
	public ShippedBooks getDataFromPublisher(){
		
		System.out.println("get method");
		
		Client client = Client.create();
		WebResource webResource = client.resource("http://54.215.210.214:9000/orders/69676");		
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
		ShippedBooks shippedBooks = response.getEntity(ShippedBooks.class);
		System.out.println(response.getStatus());
		return shippedBooks;
	}
	
	public String createMessage (Book shippedBook){
		
		String message = shippedBook.getIsbn()+":"+shippedBook.getTitle()+":"+shippedBook.getCategory()+":"+shippedBook.getCoverimage();
		
		return message;
	}
	
	
    private static String env(String key, String defaultValue) {
	String rc = System.getenv(key);
	if( rc== null ) {
	    return defaultValue;
	}
	return rc;
    }

    private static String arg(int index, String defaultValue) {
	    return defaultValue;	
    }

	

}
