package edu.sjsu.cmpe.procurement.api.resources;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;


@Every("1s")
public class ProcurementResource extends Job {

	@Override
	public void doJob(){
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
	Session session;
	try {
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	} catch (JMSException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	Destination dest = new StompJmsDestination(destination);

	MessageConsumer consumer = session.createConsumer(dest);
	System.out.println("Waiting for messages from " + queue + "...");
	while(true) {
	    Message msg = consumer.receive();
	    if( msg instanceof  TextMessage ) {
		String body = ((TextMessage) msg).getText();
		if( "SHUTDOWN".equals(body)) {
		    break;
		}
		System.out.println("Received message = " + body);

	    } else if (msg instanceof StompJmsMessage) {
		StompJmsMessage smsg = ((StompJmsMessage) msg);
		String body = smsg.getFrame().contentAsString();
		if ("SHUTDOWN".equals(body)) {
		    break;
		}
		System.out.println("Received message = " + body);

	    } else {
		System.out.println("Unexpected message type: "+msg.getClass());
	    }
	}
	connection.close();
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
