package com.fjcx.e175;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SNSActions;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

/**
/**
 * E-175 Cloud Computing
 * Assignment 6
 * Problem 6
 * Student: Frank O'Connor
 * Date: 10/16/12
 *
 * Class is Rajiv Mote's code and demo of how to subscribe an SQS queue to an SNS Topic
 */

public class A6Prob6 {
	private static final Log LOG = LogFactory.getLog(A6Prob6.class);
	private static final String TOPIC_NAME = "FjcxTestTopic";
	private static final String QUEUE_NAME = "FjcxTestQueue";

	/**
	* This program demonstrates how to hook up an AWS SQS queue to an AWS SNS
	* topic to receive messages.
	* 
	* 1. Create an SNS topic.
	* 2. Set a policy on the topic to allow subscriptions.
	* 3. Create an SQS queue.
	* 4. Set a policy on the queue to allow message posting.
	* 5. Subscribe the queue to the topic.
	* 6. Wait for AWS settings to propagate and "take."
	* 7. Send an SNS notification message.
	* 8. Listen for the message on the queue.
	* 
	* @param args not used
	*/
	public static void main(String[] args) {
		AWSCredentials awsCredentials = null;
		AmazonSNS sns = null;
		AmazonSQS sqs = null;
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		String subscriptionArn = null;
		LOG.debug("Beginning.");
		try {
			awsCredentials = 
			new PropertiesCredentials(
			A6Prob6.class.getResourceAsStream("/AwsCredentials.properties"));
			sns = new AmazonSNSClient(awsCredentials);
			
			// 1. Create a topic
			System.out.println("Step 1. Create an SNS topic.");
			CreateTopicResult ctResult = sns.createTopic(new CreateTopicRequest(TOPIC_NAME));
			topicArn = ctResult.getTopicArn();
			System.out.println(String.format("Created topic %s with ARN %s", 
			TOPIC_NAME, topicArn));
			
			// 2. Set policy on topic to allow open subscriptions
			System.out.println("Step 2. Set a policy on the topic to allow subscriptions.");
			Policy snsPolicy = new Policy().withStatements(new Statement(Effect.Allow)
				.withPrincipals(Principal.AllUsers)
				.withActions(SNSActions.Subscribe));
			System.out.println("Set SNS policy: " + snsPolicy.toJson());
			sns.setTopicAttributes(new SetTopicAttributesRequest(topicArn, "Policy", snsPolicy.toJson()));
			
			// 3. Create a queue
			System.out.println("Step 3. Create an SQS queue.");
			sqs = new AmazonSQSClient(awsCredentials);
			CreateQueueResult cqResult = sqs.createQueue(new CreateQueueRequest(QUEUE_NAME));
			queueUrl = cqResult.getQueueUrl();
			System.out.println(String.format("Created queue %s with URL %s", QUEUE_NAME, queueUrl));
			GetQueueAttributesResult queueArnResult = sqs.getQueueAttributes(new GetQueueAttributesRequest(queueUrl).withAttributeNames("QueueArn"));
			queueArn = queueArnResult.getAttributes().get("QueueArn");
			System.out.println("Queue ARN = " + queueArn);
			
			// 4. Set the queue policy to allow SNS to publish messages
			System.out.println("Step 4. Set a policy on the queue to allow message posting.");
			Policy sqsPolicy = 
			new Policy().withStatements(new Statement(Effect.Allow)
				.withPrincipals(Principal.AllUsers)
				.withResources(new Resource(queueArn)) // Note: queue, not topic
				.withActions(SQSActions.SendMessage)
				.withConditions(ConditionFactory.newSourceArnCondition(topicArn)));
			Map queueAttributes = new HashMap();
			queueAttributes.put("Policy", sqsPolicy.toJson());
			sqs.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, queueAttributes));
			System.out.println("Set SQS policy to " + queueUrl + ": " + sqsPolicy.toJson());
			
			// 5. Subscribe the queue to the topic 
			System.out.println("Step 5. Subscribe the queue to the topic.");
			SubscribeResult sResult = sns.subscribe(new SubscribeRequest(topicArn, "sqs", queueArn));
			subscriptionArn = sResult.getSubscriptionArn();
			System.out.println("Subscription ARN: " + subscriptionArn);
			
			// 6. Wait a bit for AWS to get all synched-up
			System.out.println("Step 6. Wait for AWS settings to propagate and \"take.\"");
			Thread.sleep(60000L);
			// 6.1. Verify queue attributes
			GetQueueAttributesResult gqaResult =
			sqs.getQueueAttributes(new GetQueueAttributesRequest(queueUrl).withAttributeNames("Policy", "QueueArn", "ApproximateNumberOfMessages"));
			if (gqaResult.getAttributes().size() == 0) {
				System.out.println("Queue " + QUEUE_NAME + " has no attributes");
			} else {
				System.out.println("Attributes for " + QUEUE_NAME);
				for (String key : gqaResult.getAttributes().keySet()) {
					System.out.println(String.format("\t%s = %s", key, gqaResult.getAttributes().get(key)));
				}
			}
			
			// 6.2. Verify topic attributes
			GetTopicAttributesResult gtaResult = sns.getTopicAttributes(new GetTopicAttributesRequest(topicArn));
			if (gtaResult.getAttributes().size() == 0) {
				System.out.println("Topic " + TOPIC_NAME + " has no attributes");
			} else {
				System.out.println("Attributes for " + TOPIC_NAME);
				for (String key : gtaResult.getAttributes().keySet()) {
					System.out.println(String.format("\t%s = %s", key, gtaResult.getAttributes().get(key)));
				}
			}
			
			// 6.3. Verify subscription
			ListSubscriptionsByTopicResult lsbtResult = sns.listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest(topicArn));
			if (lsbtResult.getSubscriptions().size() == 0) {
				System.out.println("Topic " + TOPIC_NAME + " has no subscriptions.");
			} else {
				System.out.println("Subscriptions for " + TOPIC_NAME);
				for (Subscription subscription : lsbtResult.getSubscriptions()) {
					System.out.println("\t" + subscription.getProtocol() + ": " + subscription.getEndpoint());
				}
			}
			
			// 7. Send a notification
			System.out.println("Step 7. Send an SNS notification message.");
			PublishResult pResult = sns.publish(new PublishRequest(topicArn, "Mr Watson -- Come here -- I want to see you."));
			System.out.println("Sent message ID = " + pResult.getMessageId());
			
			// 8. Wait for message receipt in queue
			System.out.println("Step 8. Listen for the message on the queue.");
			for (int i = 0; i < 10; i++) { 
				Thread.sleep(2000L); 
				ReceiveMessageResult rmResult = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl)); 
				if (rmResult.getMessages().size() > 0) {
					// A message has been received
					for (Message message : rmResult.getMessages()) {
						System.out.println(message.getBody());
						sqs.deleteMessage(new DeleteMessageRequest(queueUrl, 
						message.getReceiptHandle()));
					}
					break;
				} else {
					// ??? Why aren't we receiving messages?
					System.out.println("No messages available, attempt " + (i+1));
				}
			}
		} catch (Exception e) {
			e.printStackTrace(System.err);
		} finally {
			System.out.println("Shutting down...");
			// Unsubscribe the queue from the topic
			if (sns != null && subscriptionArn != null) {
				sns.unsubscribe(new UnsubscribeRequest(subscriptionArn));
				System.out.println("Unsubscribed queue from topic.");
			}
			// Destroy queue
			if (sqs != null && queueUrl != null) {
				sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
				System.out.println("Deleted the queue.");
				sqs.shutdown();
			}
			// Destroy topic
			if (sns != null && topicArn != null) {
				sns.deleteTopic(new DeleteTopicRequest(topicArn));
				System.out.println("Deleted the topic.");
				sns.shutdown();
			}
		}
		LOG.debug("Done.");
	}

}
