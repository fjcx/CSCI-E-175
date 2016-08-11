package com.fjcx.e175;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.IpAddressCondition;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

/**
/**
 * E-175 Cloud Computing
 * Assignment 6
 * Problem 3
 * Student: Frank O'Connor
 * Date: 10/16/12
 *
 * Class creates an SQS queue, then tries to create a policy to restrict to the SQS queue by ip address
 */
public class A6Prob3 {
	
	private static String queueName = "fjcxProb3Queue";
	public static AmazonSQS sqs = null;

    public static void main(String[] args) throws Exception {

        sqs = new AmazonSQSClient(new PropertiesCredentials(
                A6Prob3.class.getResourceAsStream("/AwsCredentials.properties")));

        try {
            // Create a queue
            System.out.println("Creating a new SQS queue called: "+ queueName +"\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);

            String prob3QueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            // Get queue attributes
            System.out.println("Retrieveing attibutes for queue: "+ queueName +"\n");
            GetQueueAttributesRequest getQARequest = new GetQueueAttributesRequest().withQueueUrl(prob3QueueUrl);
            GetQueueAttributesResult getQAResult = sqs.getQueueAttributes(getQARequest.withAttributeNames("QueueArn"));
            Map<String,String> attributeMap = getQAResult.getAttributes();
 
            // List queue attributes
            for (String key : attributeMap.keySet()) {
	        	   System.out.println(key + ": " + attributeMap.get(key));
	        }
            
        	// Setup policy to restrict ip address for vpn		-- not working for vpn ip address - because user is root
            IpAddressCondition deny = new IpAddressCondition("140.000.00.188/32");
            // Setup policy to restrict own ip address		-- not working for own ip address - because user is root 
            // IpAddressCondition deny = new IpAddressCondition("50.000.001.55/32");
            
            System.out.println("Creating new policy that will restrict certain Ip Addresses\n");
            Policy policy = new Policy("restrictIpAddress").withStatements(new Statement(Effect.Deny)
            	.withPrincipals(Principal.AllUsers)
            	.withActions(SQSActions.AllSqsActions)
            	.withResources(new Resource(attributeMap.get("QueueArn")))
            	.withConditions(deny));

            Map<String,String> queueAttributes = new HashMap<String,String>();
            
            // Add the policy to the existing attributesMap
            queueAttributes.put("Policy", policy.toJson());

            // Update the queues attributes
            sqs.setQueueAttributes(new SetQueueAttributesRequest(prob3QueueUrl, queueAttributes));

            // List queues
            System.out.println("Listing all queues in your account.\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl +"\n");
            }

            // Try to send message -- assuming restricted by ip-address for vpn
            System.out.println("Try to send message via restricted ip address: "+ queueName +"\n");
            sqs.sendMessage(new SendMessageRequest(prob3QueueUrl, "This is prob3 message1"));

            // Receive message
            retreiveMessages(prob3QueueUrl);
            
            // Delete the queue
            System.out.println("Deleting the queue called: "+ queueName +"\n");
            sqs.deleteQueue(new DeleteQueueRequest(prob3QueueUrl));
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
