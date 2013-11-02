package edu.sjsu.cmpe.library;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryName;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;

public class LibraryService extends Service<LibraryServiceConfiguration> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private String libraryName = "library-a";
    
    

    public String getLibraryName() {
                return libraryName;
        }

        public void setLibraryName(String libraryName) {
                this.libraryName = libraryName;
        }

        public static void main(String[] args) throws Exception {
        new LibraryService().run(args);
    }

    @Override
    public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {
        bootstrap.setName("library-service");
        bootstrap.addBundle(new ViewBundle());
        
        
        
        
        Thread one = new Thread(new Runnable(){
                public void run(){
                        try {
                                startListener();
                        } catch (JMSException e) {
                                e.printStackTrace();
                        }
                }
        });
        one.start();
    }

        public void startListener() throws JMSException{
                    
                    String libraryName = LibraryName.libraryName;
                    String user = env("APOLLO_USER", "admin");
                    String password = env("APOLLO_PASSWORD", "password");
                    String host = env("APOLLO_HOST", "54.215.210.214");
                    int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
                    String destination = null;
                    if(libraryName.equals("library-a"))
                            destination = arg(0, "/topic/69676.book.*");
                    else
                            destination = arg(0, "/topic/69676.book.computer");                           
                                    
        
                    StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
                    factory.setBrokerURI("tcp://" + host + ":" + port);
        
                    Connection connection = factory.createConnection(user, password);
                    connection.start();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination dest = new StompJmsDestination(destination);
        
                    MessageConsumer consumer = session.createConsumer(dest);
                    System.currentTimeMillis();
                    System.out.println("Waiting for messages...");
                    String body = new String();
                    while(true) {
                        Message msg = consumer.receive();
                        if( msg instanceof  TextMessage ) {
                            body = ((TextMessage) msg).getText();
                            if( "SHUTDOWN".equals(body)) {
                                break;
                            }
                            System.out.println("Received message = " + body);
        
                        } else if (msg instanceof StompJmsMessage) {
                            StompJmsMessage smsg = ((StompJmsMessage) msg);
                            body = smsg.getFrame().contentAsString();
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
    
        
    @Override
    public void run(LibraryServiceConfiguration configuration,
            Environment environment) throws Exception {
            
        String queueName = configuration.getStompQueueName();
        String topicName = configuration.getStompTopicName();
        
        //setLibraryName(configuration.getLibraryName());
        LibraryName.libraryName = configuration.getLibraryName();
        
        log.debug("Queue name is {}. Topic name is {}", queueName,topicName);
        log.debug("Library name is {}",libraryName);

        /** Root API */
        environment.addResource(RootResource.class);
        /** Books APIs */
        BookRepositoryInterface bookRepository = new BookRepository();
        //environment.addResource(new BookResource(bookRepository,libraryName));

        /** UI Resources */
        environment.addResource(new HomeResource(bookRepository));
    }
}