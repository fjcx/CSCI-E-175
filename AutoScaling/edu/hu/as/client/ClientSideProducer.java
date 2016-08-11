package edu.hu.as.client;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * sends a message to user queue (see below in code!) every WAIT_MSG_ms milisecond
 */
public class ClientSideProducer {
    static final int WAIT_MSG_ms   = 4000;
	
    public static void main(String[] args) throws Exception {
	
	/* replace with your access key and secret key */
        AmazonSQS sqs = new AmazonSQSClient( new BasicAWSCredentials( "A...................Q", "g..........................E") );

        System.out.println("===========================================");
        System.out.println("PRODUCER");
        System.out.println("===========================================\n");

        try {
        	while (true) {
        		String msg = "Message for auto scaling example at: " 
				+ (new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format( new Date() ) );
        		System.out.println("Sending a message: " + msg + "\n");
			/* replace with your SQS QUEUE URL */
			sqs.sendMessage( new SendMessageRequest( "https://queue.amazonaws.com/951414139794/AutoScalingTestQueue", msg ) );
			Thread.sleep( WAIT_MSG_ms );
        	}
        	//https://queue.amazonaws.com/951414139794/AutoScalingTestQueue

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
}
