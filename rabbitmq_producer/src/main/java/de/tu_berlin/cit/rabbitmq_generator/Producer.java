package de.tu_berlin.cit.rabbitmq_generator;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;

import org.json.JSONObject;

public class Producer {

	private static Channel channel;
	private static String queue;
	
	// for random event generation
	private static Random   rand = new Random();
	private static String[] adTypes = {"banner", "modal", "sponsored-search", "mail", "mobile"};
	private static String[] eventTypes = {"view", "click", "purchase"};
	private static String[] adIds = makeIds(10 * 100); // 10 ads per campaign
	private static String[] userIds = makeIds(100);
	private static String[] pageIds = makeIds(100);

	// returns an array of n uuids
	public static String[] makeIds(int n) {
		String[] uuids = new String[n];
		for (int i = 0; i < n; i++)
			uuids[i] = UUID.randomUUID().toString();
		return uuids;
	}

	public static JSONObject makeEvent(long time) {
						
		JSONObject event = new JSONObject();
		event.put("user_id", userIds[rand.nextInt(userIds.length)]);
		event.put("page_id", pageIds[rand.nextInt(pageIds.length)]);
		event.put("ad_id", adIds[rand.nextInt(adIds.length)]);
		event.put("ad_type", adTypes[rand.nextInt(adTypes.length)]);
		event.put("event_type", eventTypes[rand.nextInt(eventTypes.length)]);
		event.put("event_time", time);
		event.put("ip_address", "1.2.3.4");

		return event;
	}
	

	public static void run(int throughput, int duration) throws InterruptedException, IOException {
		
		System.out.println("Running, emitting " + throughput + " tuples per second for " + duration + " seconds");
		
		Long endTime = System.currentTimeMillis() + duration*1000;
		Long sendTime = System.currentTimeMillis();
		long offset = 1000 / throughput;
				
		// continue to run for duration
		while(System.currentTimeMillis() < endTime) {
			
			// send messages at configured throughput
			sendTime = sendTime + offset;
			Long cur = System.currentTimeMillis();
			if (sendTime > cur) {
				Thread.sleep(sendTime - cur);
			}
			
			// alert if falling behind
			if (cur > sendTime + 100) {
				System.out.println("Falling behind by " + (cur-sendTime) + "ms");
			}
			
			
			String message = makeEvent(sendTime).toString();
			channel.basicPublish("", queue, null, message.getBytes());
			//System.out.println(" [x] Sent '" + message + "'");
			

		}
				
	}
	
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("Usage: [throughput per second] [generator duration (s)]");
			return;
		}
		
		int throughput = Integer.parseInt(args[0]);
		int duration = Integer.parseInt(args[1]);
		
		System.out.println("Initializing...");
		
        // retrieve properties from file
        Properties config = Resources.GET.read("config.properties", Properties.class);

        String user = config.getProperty("rabbitmq.user");
        String pass = config.getProperty("rabbitmq.pass");
        String host = config.getProperty("rabbitmq.host");
        int port = Integer.parseInt(config.getProperty("rabbitmq.port"));
        queue = config.getProperty("rabbitmq.queue");
        
        // rabbitmq connection configs
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setUsername(user);
	    factory.setPassword(pass);
	    factory.setVirtualHost("/");
	    factory.setHost(host);
	    factory.setPort(port);

	    // connect to rabbitmq and create queue
	    Connection conn = factory.newConnection();
	    channel = conn.createChannel();
		channel.queueDeclare(queue, false, false, false, null);

		// generate [throughput] tupels per second for [duration] seconds
		run(throughput, duration);
		
		channel.close();
		conn.close();
		
		System.out.println("Done.");
		
	}

}
