import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;

import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
//import java.util.Date;
//import java.util.List;
import java.io.File;
//import java.util.Base64;
import java.util.*;


//import InFile;


public class Main {
    public static void main(String[] args) throws Exception {
//        // Get data from args
//        File input = new File(args[0]);
//        File output = new File(args[1]);
//        int workerRatio = Integer.parseInt(args[2]);
//        Boolean terminate = args.length == 4 && args[3].equals("terminate"); //TODO: Check with Moshe, What is 'terminate' type?
//
//        // Get s3
//        String bucket_name = "oo-dspsp-ass1";
//        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
//
//        // Upload input to S3
//        String key_name = generate_keyName();
//        s3.putObject(PutObjectRequest.builder().bucket(bucket_name).key(key_name).build(), RequestBody.fromFile(input));
//        ec2Try();
        sqsTry();
    }

    public static String generate_keyName(){
        String newKey = "inputTest";
        //TODO: Get available name
        return  newKey;
    }

    public static void ec2Try(){
        Ec2Client ec2 = Ec2Client.create();
        String amiId = "ami-00e95a9222311e8ed";

//        ami without java - ami-04ad2567c9e3d7893 (T1_MICRO)
//        ami with java - ami-00e95a9222311e8ed (T2_MICRO)

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .keyName("key2")
                .userData(Base64.getEncoder().encodeToString(
                                ("#!/bin/bash\n" + "set -x\n" + "echo hello world").getBytes()))
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();
//      Print a list of available instances
        System.out.println(instances);

        String id = instances.get(0).instanceId();
        GetConsoleOutputResponse res = GetConsoleOutputResponse.builder().instanceId(id).build();

        System.out.println(res);
        System.out.println(res.output());

    }

    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();

    public static void sqsTry(){
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();


        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(QUEUE_NAME)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("hello world")
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);


        // Send multiple messages to the queue
        SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(
                        SendMessageBatchRequestEntry.builder()
                                .messageBody("Hello from message 1 - Ohad")
                                .id("msg_1")
                                .build()
                        ,
                        SendMessageBatchRequestEntry.builder()
                                .messageBody("Hello from message 2 - Ori")
                                .delaySeconds(10)
                                .id("msg_2")
                                .build())
                .build();
        sqs.sendMessageBatch(send_batch_request);

        // receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        System.out.println(messages.toString());
        for (Message m : messages) {
            System.out.println(m.body());
        }

//         delete messages from the queue
        for (Message m : messages) {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteRequest);
        }
    }

}

