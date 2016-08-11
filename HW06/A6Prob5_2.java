package com.fjcx.e175;

import java.util.List;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.amazonaws.services.sns.model.ConfirmSubscriptionResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.UnsubscribeRequest;

/**
 * E-175 Cloud Computing
 * Assignment 6
 * Problem 5 part 2
 * Student: Frank O'Connor
 * Date: 10/16/12
 *
 * Class confirms endpoint topic subscription, publishes message to topic. 
 * Then deletes topic
 */
public class A6Prob5_2 {
	
	private static String topicName = "fjcxProb5Topic";
	public static AmazonSNS sns = null;

    public static void main(String[] args) throws Exception {

        sns = new AmazonSNSClient(new PropertiesCredentials(
                A6Prob5_2.class.getResourceAsStream("/AwsCredentials.properties")));

        try {
        	
        	String topicArn = "arn:aws:sns:us-east-1:992907847162:fjcxProb5Topic";
            String tokenFjo = "233641...71f";
            String tokenRck = "2336...6";
            String subject = "Emergency Message Service2";
            String msg = "Hello java notify message";
            
            // Opt in to sns subscription for both endpoints
            System.out.println("Confirming subscriptions for endpoint fjo.con@gmail.com for topic: "+ topicName +"\n");
            ConfirmSubscriptionResult conSubRes = sns.confirmSubscription(new ConfirmSubscriptionRequest(topicArn, tokenFjo));
            String subscribedTopicArnFjo  = conSubRes.getSubscriptionArn();
            System.out.println("Confirming subscriptions for endpoint ria.c.knapp@gmail.com for topic: "+ topicName +"\n");
            conSubRes = sns.confirmSubscription(new ConfirmSubscriptionRequest(topicArn, tokenRck));
            String subscribedTopicArnRck = conSubRes.getSubscriptionArn();
            
            // List Topic subscriptions
            System.out.println("Listing topic subscriptions");
            ListSubscriptionsResult listSubResult = sns.listSubscriptions();
            List<Subscription> subscriptions = listSubResult.getSubscriptions(); 
            for (Subscription sub : subscriptions) {
            	System.out.println(sub.getEndpoint() + " " + sub.getOwner() + " " + sub.getTopicArn());
            }
            
            // Publish a message
   			System.out.println("Publishing a message to all topics");
   			sns.publish(new PublishRequest().withTopicArn(topicArn).withMessage(msg).withSubject(subject));
   			
   			System.out.println("Please check email for message to confirm");
           	
   			// Unsubscribe endpoints
   			System.out.println("Unsubscribing fjo.con@gmail.com from topic: "+ topicName +"\n");
           	sns.unsubscribe(new UnsubscribeRequest().withSubscriptionArn(subscribedTopicArnFjo));
           	
   			System.out.println("Unsubscribing ria.c.knapp@gmail.com from topic: "+ topicName +"\n");
           	sns.unsubscribe(new UnsubscribeRequest().withSubscriptionArn(subscribedTopicArnRck));
           	
           	// Delete topic
           	System.out.println("Deleting topic: "+ topicName +"\n");
           	sns.deleteTopic(new DeleteTopicRequest().withTopicArn(topicArn));
           	
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
