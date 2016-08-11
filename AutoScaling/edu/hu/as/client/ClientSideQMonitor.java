 package edu.hu.as.client;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;

/**
 * prints current time and estimated length of queue (visible messages)
 * every WAIT_ms miliseconds, ad infinitum.
 */
public class ClientSideQMonitor {

    static final int WAIT_ms = 5000;
	
    public static void main(String[] args) throws Exception {
    	AmazonSQS sqs = new AmazonSQSClient( new BasicAWSCredentials( "A...............Q", "g.....................E") );

    	int wait_ms = args.length > 0 ? Integer.valueOf( args[0] ) : WAIT_ms;
    	
        System.out.println("===========================================");
        System.out.println("Monitoring SQS");
        System.out.println("===========================================\n");

        try {
	    // get queue length, provide know QUEUE URL of your Queue
            GetQueueAttributesRequest attrReq = new GetQueueAttributesRequest( 
             "https://queue.amazonaws.com/951414139794/AutoScalingTestQueue"); //"Queue URL"
            attrReq.setAttributeNames( Arrays.asList(   
            		"ApproximateNumberOfMessages", 
            		"ApproximateNumberOfMessagesNotVisible"
            		));
            
            while (true) {
            	GetQueueAttributesResult attrResult = sqs.getQueueAttributes(
            			attrReq );
            	Map<String,String> attrs = attrResult.getAttributes();
            	System.out.println(
            			(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format( new Date() ) )
            			+ ": " + attrs.get("ApproximateNumberOfMessages") );
            	Thread.sleep( wait_ms );
            }
            
            
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
