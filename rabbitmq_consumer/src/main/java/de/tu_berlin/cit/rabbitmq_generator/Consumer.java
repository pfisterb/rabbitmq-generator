package de.tu_berlin.cit.rabbitmq_generator;

import java.util.Properties;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;


public class Consumer {

	public static void main(String[] args) throws Exception {
		
		System.out.println("Initializing...");
		
        // retrieve properties from file
        Properties config = Resources.GET.read("config.properties", Properties.class);

        String queue = config.getProperty("rabbitmq.queue");
        String user = config.getProperty("rabbitmq.user");
        String pass = config.getProperty("rabbitmq.pass");
        String host = config.getProperty("rabbitmq.host");
        int port = Integer.parseInt(config.getProperty("rabbitmq.port"));
        
        // rabbitmq connection configs
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setUsername(user);
	    factory.setPassword(pass);
	    factory.setVirtualHost("/");
	    factory.setHost(host);
	    factory.setPort(port);

	    Connection conn = factory.newConnection();
	    Channel channel = conn.createChannel();
	    channel.queueDeclare(queue, false, false, false, null);
	    
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

	    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
	        String message = new String(delivery.getBody(), "UTF-8");
	        System.out.println(" [x] Received '" + message + "'");
	    };
	    channel.basicConsume(queue, true, deliverCallback, consumerTag -> { });
	    		
	}

}
