package com.fjcx.e175;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * E-175 Cloud Computing
 * Assignment 6
 * Problem 2
 * Student: Frank O'Connor
 * Date: 10/16/12
 *
 * Class creates a queue, then tries to send a message of size 72k to the queue. Expecting program to fail at this step as messages should not be
 * larger than 64k
 */
public class A6Prob2 {
	
	private static String queueName = "fjcxProb2Queue";
	public static AmazonSQS sqs = null;

    public static void main(String[] args) throws Exception {

        sqs = new AmazonSQSClient(new PropertiesCredentials(
                A6Prob2.class.getResourceAsStream("/AwsCredentials.properties")));

        try {
        	// read in file (of size 72k) into string
        	String str72k = readFile("bigFile72k.txt");

            // Create an SQS queue
            System.out.println("Creating a new SQS queue called: "+ queueName +"\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            		
            String prob2QueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            // List queues
            System.out.println("Listing all queues in your account.\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl +"\n");
            }

            // Try to send message with 72k string
            System.out.println("Try to send message with 72k size to queue: "+ queueName +"\n");
            sqs.sendMessage(new SendMessageRequest(prob2QueueUrl, str72k));

            // Receive message -- *** assuming prog has failed by this point ***
            retreiveMessages(prob2QueueUrl);
            
            // Delete the queue
            System.out.println("Deleting the queue called: "+ queueName +"\n");
            sqs.deleteQueue(new DeleteQueueRequest(prob2QueueUrl));
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
    
    private static String readFile(String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");

        while((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        reader.close();
        return stringBuilder.toString();
    }
}
