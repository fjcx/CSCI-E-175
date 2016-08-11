package com.fjcx.e175;

import java.util.HashMap;
import java.util.Map;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Topic;

/**
 * E-175 Cloud Computing
 * Assignment 6
 * Problem 5 part 1
 * Student: Frank O'Connor
 * Date: 10/16/12
 *
 * Class creates and SNS topic and sends subscription to two emails.
 */
public class A6Prob5_1 {
	
	private static String topicName = "fjcxProb5Topic";
	public static AmazonSNS sns = null;

    public static void main(String[] args) throws Exception {

        sns = new AmazonSNSClient(new PropertiesCredentials(
                A6Prob5_1.class.getResourceAsStream("/AwsCredentials.properties")));

        try {

            // Create a topic
            System.out.println("Creating a new SNS topic called: "+ topicName +"\n");
            CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName); 

            // Retrieve Amazon Resource Name
            String SNSTopicArn = sns.createTopic(createTopicRequest).getTopicArn();
            
            // List queues
            System.out.println("Listing all queues in user account.\n");
            for (Topic topic : sns.listTopics().getTopics()) {
            	System.out.println(" TopicArn: " + topic.getTopicArn());
            }

            // Fetch topic attributes
            System.out.println("Fecthing attributes for topic: "+ topicName +"\n");
            GetTopicAttributesRequest getTARequest = new GetTopicAttributesRequest().withTopicArn(SNSTopicArn);
            GetTopicAttributesResult getTAResult = sns.getTopicAttributes(getTARequest);
            
            Map<String, String> attributes = new HashMap<String, String>();
            attributes = getTAResult.getAttributes();
            for (String key : attributes.keySet()) {
            	System.out.println(key + ": " + attributes.get(key));
            }
            
            // Subscribe both emails to topic
            System.out.println("Subscribing email ....@... to topic: "+ topicName);
            SubscribeRequest subReq = new SubscribeRequest(SNSTopicArn,"email","....@...");
            SubscribeResult subRes = sns.subscribe(subReq);
            String subscribedTopicArn = subRes.getSubscriptionArn();
            
            System.out.println("Subscribing email ....@... to topic: "+ topicName);
            subReq = new SubscribeRequest(SNSTopicArn,"email","....@...");
            subRes = sns.subscribe(subReq);
            subscribedTopicArn = subRes.getSubscriptionArn();

            // Inform user of subscription email
            System.out.println("Subscription emails are sent, please note token from subscription link and run part 2 with appropriate tokens");

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SNS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SNS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
