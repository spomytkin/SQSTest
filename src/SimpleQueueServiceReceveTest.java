/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

/**
 *Modification of Amazon sample to test SendMessageBatchRequest
 */
public class SimpleQueueServiceReceveTest {

    private static final int TOTAL_MESSAGES_NUM = 1000;
    private static final int MAX_NUM_BATCH_MESSAGES = 10;
    private static final long POLLING_INTERVAL =0;// 5 * 1000; // milliseconds
    private static final String[] QUEUE_ATTRUBUTE_REQUEST = { "All" };

    public static void main(String[] args) throws Exception {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         * AwsCredentials.properties file before you try to run this sample.
         * http://aws.amazon.com/security-credentials
         */
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(
                SimpleQueueServiceReceveTest.class.getResourceAsStream("AwsCredentials.properties")));

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");

        try {
            // Create a queue
            System.out.println("Creating a new SQS queue called MyQueue.\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

            GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(
                    myQueueUrl);
            getQueueAttributesRequest.setAttributeNames(new HashSet<String>(Arrays
                    .asList(QUEUE_ATTRUBUTE_REQUEST)));
            GetQueueAttributesResult getQueueAttributesResult = sqs
                    .getQueueAttributes(getQueueAttributesRequest);

            System.out.println("Queue Attributes :" + getQueueAttributesResult);

            // List queues
            System.out.println("Listing all queues in your account.\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();

            // Send a message
    
            System.out.println("Receiving messages from MyQueue.\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
                    .withMaxNumberOfMessages(MAX_NUM_BATCH_MESSAGES);
            System.out.println("Max num of Messages = "
                    + receiveMessageRequest.getMaxNumberOfMessages());

            int totalNumOfMessagesReceived = 0;

            long startTime = System.currentTimeMillis();
            while (totalNumOfMessagesReceived < TOTAL_MESSAGES_NUM) {
                Thread.sleep(POLLING_INTERVAL);
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                List<DeleteMessageBatchRequestEntry>  entries=new ArrayList<DeleteMessageBatchRequestEntry>()
                for (Message message : messages) {
            /*        System.out.println("  Message");
                    System.out.println("    MessageId:     " + message.getMessageId());
                    System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                    System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                    System.out.println("    Body:          " + message.getBody());
                    for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                        System.out.println("  Attribute");
                        System.out.println("    Name:  " + entry.getKey());
                        System.out.println("    Value: " + entry.getValue());
                    }*/
                	DeleteMessageBatchRequestEntry entry;
					entries.add(entry);
                    totalNumOfMessagesReceived++;                    		
                }
				sqs.deleteMessageBatch(new DeleteMessageBatchRequest(myQueueUrl, entries));

            }
            System.out.println(String.format("It took %.2f seconds to receive and delete %d messages  by batch of  %d ",
                    (float) (System.currentTimeMillis() - startTime) / 1000, TOTAL_MESSAGES_NUM, MAX_NUM_BATCH_MESSAGES));
            System.out.println();

            // Delete a queue
   //         System.out.println("Deleting the test queue.\n");
  //          sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
        } catch (AmazonServiceException ase) {
            System.out
                    .println("Caught an AmazonServiceException, which means your request made it "
                            + "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out
                    .println("Caught an AmazonClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with SQS, such as not "
                            + "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}