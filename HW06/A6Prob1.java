package com.fjcx.e175;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * E-175 Cloud Computing
 * Assignment 6
 * Problem 1
 * Student: Frank O'Connor
 * Date: 10/16/12
 *
 * Class creates and SQS queue with a default visibility timeout set at 20 seconds. Sends a message, retrieves it then deletes it. Sends a second message, retrieves it, sleeps for 30 sec
 * then retrieves it again after it reappears in the queue. Adds two more messages to the queue, executes GetQueueAttributes call to find how many messages are in queue, and retrieves all 
 * attributes in the queue.
 */
public class A6Prob1 {
	
	private static String queueName = "fjcxProb1Queue";
	public static AmazonSQS sqs = null;

    public static void main(String[] args) throws Exception {

        sqs = new AmazonSQSClient(new PropertiesCredentials(
                A6Prob1.class.getResourceAsStream("/AwsCredentials.properties")));

        try {
        	
        	// Setting endpoint to HTTP so we can capture traffic
        	sqs.setEndpoint("http://queue.amazonaws.com/");
        	
            // Create a queue and set the default Visibility Timeout to 20 seconds
        	// Noted: withDefaultVisibilityTimeout() has been removed from aws-java-sdk and must use this method
            System.out.println("Creating a new SQS queue called: "+ queueName +"\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName)
            		.withAttributes(Collections.singletonMap(QueueAttributeName.VisibilityTimeout.name(), String.valueOf("20")));
            String prob1QueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            // List queues
            System.out.println("Listing all queues in user account.\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl +"\n");
            }

            // Send first message ("message1")
            System.out.println("Sending message1 to queue: "+ queueName +"\n");
            sqs.sendMessage(new SendMessageRequest(prob1QueueUrl, "This is message1."));

            // Receive first message
            List<Message> messages = retreiveMessages(prob1QueueUrl);
            
            // Delete first message ("message1")
            System.out.println("Deleting message1.\n");
            String messageRecieptHandle = messages.get(0).getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(prob1QueueUrl, messageRecieptHandle));
            
            
            // Send second message ("message2")
            System.out.println("Sending message2 to queue: "+ queueName +"\n");
            sqs.sendMessage(new SendMessageRequest(prob1QueueUrl, "This is message2."));

            // Receive second message
            messages = retreiveMessages(prob1QueueUrl);
            
            // Sleep for 30 seconds
            System.out.println("sleeping for 30 seconds to wait out timeout");
            Thread.sleep(30000);
            System.out.println("waking after sleep");
            
            // Receive second message again
            messages = retreiveMessages(prob1QueueUrl);
            
            // Sending two more messages to the queue ("message3" and "message4")
            System.out.println("Sending message3 and message4 to queue: "+ queueName +"\n");
            sqs.sendMessage(new SendMessageRequest(prob1QueueUrl, "This is message3."));
            sqs.sendMessage(new SendMessageRequest(prob1QueueUrl, "This is message4."));
            
            // Fetch all Queue attribute ApproximateNumberOfMessages
            GetQueueAttributesRequest getQARequest = new GetQueueAttributesRequest().withQueueUrl(prob1QueueUrl);
            
            
           // GetQueueAttributesResult getQAResult = sqs.getQueueAttributes(getQARequest.withAttributeNames("ApproximateNumberOfMessages"));
            GetQueueAttributesResult getQAResult = sqs.getQueueAttributes(getQARequest.withAttributeNames("All"));
            Map<String,String> attributeMap = getQAResult.getAttributes();

            // Display all attributes for the queue
            System.out.println("\nQueue attributes for queue " + prob1QueueUrl + ":");
            Set<String> keys = attributeMap.keySet();

            for (String key : keys){
			    String value = (String) attributeMap.get(key);
			    System.out.println(key.toString() + " = " + value);
			}

            // Delete the queue
            System.out.println("Deleting the queue called: "+ queueName +"\n");
            sqs.deleteQueue(new DeleteQueueRequest(prob1QueueUrl));
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
    
    public static List<Message> retreiveMessages(String queueUrl){
        System.out.println("Receiving messages from queue: "+ queueName +"\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
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
        }
        return messages;
    }
}
