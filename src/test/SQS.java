package test;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.github.kevinsawicki.http.HttpRequest;

class ThreadPool
{
    private final BlockingQueue<Runnable> workerQueue;
    private final Thread[] workerThreads;
    private volatile boolean shutdown;
 
    public ThreadPool(int N)
    {
        workerQueue = new LinkedBlockingQueue<>();
        workerThreads = new Thread[N];
 
        //Start N Threads and keep them running
        for (int i = 0; i < N; i++) {
            workerThreads[i] = new Worker("Pool Thread " + i);
            workerThreads[i].start();
        }
    }
 
    public void addTask(Runnable r)
    {
        try {
            workerQueue.put(r);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
 
    public void shutdown()
    {
        while (!workerQueue.isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //interruption
            }
        }
        shutdown = true;
        for (Thread workerThread : workerThreads) {
            workerThread.interrupt();
        }
    }
 
    private class Worker extends Thread
    {
        public Worker(String name)
        {
            super(name);
        }
 
        public void run()
        {
            while (!shutdown) {
                try {
                    //each thread wait for next runnable and executes it's run method
                    Runnable r = workerQueue.take();
                    r.run();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
    }
}

public class SQS {
    static AmazonEC2      ec2;
    
	public static AmazonSQS sqs;
	public static AmazonSNS sns;
	public static String myQueueUrl;
	public static ThreadPool pool = new ThreadPool(20);
	private final static BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10);
	
	private void init()
    {
        pool.addTask(new Producer());
    }
	
    public static void main(String[] args) throws Exception {

        System.out.println("--Reading credential information...");
    	AWSCredentials credentials = new PropertiesCredentials(
    	    SQS.class.getResourceAsStream("AwsCredentials.properties"));
    	System.out.println("--Read credential information.");
    	
        sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        
        sns = new AmazonSNSClient(credentials);
        sns.setRegion(usWest2);

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");

        SQS sqs_object = new SQS();
        while(true){
            try {
            	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest("");
            	List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                for (Message message : messages) {
                    System.out.println("  Message");
                    System.out.println("    MessageId:     " + message.getMessageId());
                    System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                    System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                    System.out.println("    Body:          " + message.getBody());
                    for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                        System.out.println("  Attribute");
                        System.out.println("    Name:  " + entry.getKey());
                        System.out.println("    Value: " + entry.getValue());
                    }
                    queue.put(message.getBody());
                    sqs.deleteMessage(new DeleteMessageRequest("", message.getReceiptHandle()));
                }
                if (queue.size() !=0)
                	sqs_object.init();
                
            } catch (AmazonServiceException ase) {
                System.out.println("Caught an AmazonServiceException, which means your request made it " +
                        "to Amazon SQS, but was rejected with an error response for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());
            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which means the client encountered " +
                        "a serious internal problem while trying to communicate with SQS, such as not " +
                        "being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());
            }
        }        
        //pool.shutdown();
    }
    
    private class Producer implements Runnable
    {
        @Override
        public void run()
        {
            try {
            	String content = queue.poll();
            	System.out.println(content);
                String[] tokens= content.split("::");
                String sentiment = HttpRequest.get("http://access.alchemyapi.com/calls/text/TextGetTextSentiment?apikey=&text=" + tokens[0] + "&outputMode=json").body();
    			System.out.println(sentiment);
                int start_pos = sentiment.indexOf("type");
    			int end_pos = sentiment.indexOf("score");
    			String stype;
    			if (end_pos != -1)
    				stype = sentiment.substring(start_pos+8, start_pos+16);
    			else
    				stype = "neutral";
    			System.out.println(stype);
    			System.out.println(tokens[5]);
    			System.out.println(tokens[6]);
    			sns.publish("", tokens[5] + "," + tokens[6] + "," + stype);
    			
                Thread.sleep(1000);
            } catch (InterruptedException e2) {
                Thread.currentThread().interrupt();
            }
        }
    }
}