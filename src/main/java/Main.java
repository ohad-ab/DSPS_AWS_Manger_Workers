import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
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
import javax.naming.Name;
import java.io.File;
//import java.util.Base64;
import java.util.*;


//import InFile;


public class Main {
    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
    private static final SqsClient sqs = createSQS();
    private static boolean terminate=true;

    public static void main(String[] args) throws Exception {
        // Get data from args
        File input = new File(args[0]);
//        File output = new File(args[1]);
        int workerRatio = Integer.parseInt(args[2]);
//        Boolean terminate = args.length == 4 && args[3].equals("terminate"); //TODO: Check with Moshe, What is 'terminate' type?

        // Get s3
//        String bucket_name = "oo-dspsp-ass1";
        String bucket_name = "dsps-221";

        // Upload input to S3
        String key_name = generate_keyName();
        //s3.putObject(PutObjectRequest.builder().bucket(bucket_name).key(key_name).build(), RequestBody.fromFile(input));

        //create sqs

    //    S3Object obj = s3.getObject(GetObjectRequest.builder().build());
        sendMessage(sqs,"s3://"+bucket_name+"/"+key_name+"\t"+workerRatio);
        GetQueueUrlResponse getQueueUrlResponse =
                sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build());
        String queueUrl = getQueueUrlResponse.queueUrl();
        String managerJarLoc =""; //TODO: get from s3
        String workerJarLoc =""; //TODO: get from s3
        Ec2Client ec2 = Ec2Client.create();
        if (!isRunningEc2(ec2)){
            runNewManager(ec2,managerJarLoc, workerJarLoc);
        }

        waitForMessage(queueUrl);
        if(terminate)
        {
            sendMessage(sqs,"terminate");
        }
        // ec2Try();
       // sqsTry();
    }

    private static void runNewManager(Ec2Client ec2,String managerJar, String workerJar ) {
        System.out.println("Running new manager");
        String amiId = "ami-00e95a9222311e8ed";
        String javaInstallation = "wget --no-check-certificate --no-cookies --header \"Cookie: oraclelicense=accept-securebackup-cookie\" http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.rpm\n"+
                "sudo yum install -y jdk-8u141-linux-x64.rpm\n";
        //sg-0a245fb00956df9ba security group ohad
        //sg-0f83f8c78a44f97a6 security group ori
        IamInstanceProfileSpecification iam = IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build();//.name("EMR_EC2_DefaultRole").build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(iam)
                .userData(Base64.getEncoder().encodeToString(
                        ("#!/bin/bash\n"+
                                //javaInstallation+
                         //       managerJar
                                "aws s3 cp s3://dsps-221/Manager.jar Manager.jar\n" +
                                "java -jar Manager.jar\n"
                                ).getBytes()))
                .build();
//        String reservation_id = ec2.describeInstances().reservations().get(0).instances().get(0).instanceId();
        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        ec2.createTags(CreateTagsRequest.builder().resources(instanceId).tags(Tag.builder()
                .key("Name").value("Manager").build()).build());



//        List<Reservation> reservList = ec2.describeInstances().reservations();
//        List<Instance> instanceList =  reservList.get(0).instances();
        //RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();
        System.out.println(instances);
    }

    private static boolean isRunningEc2(Ec2Client ec2) {
        List<Reservation> reservations = ec2.describeInstances().reservations();
        for (Reservation reservation:reservations) {
            Instance instance = reservation.instances().get(0);
            if(!instance.tags().isEmpty() && instance.tags().get(0).value().equals("Manager"))
                if(instance.state().nameAsString().equals("pending")){
                    System.out.println("There is a manager pending, try again");//TODO: ask moshe about the pending status
                    return true;
                }
                else if (instance.state().nameAsString().equals("running")) {
                    System.out.println("Manager is already running");
                    return true;
                }
            }

        return false;
    }

    public static String generate_keyName(){//TODO: check if needed.
        String newKey = "inputTest";
        //TODO: Get available name
        return  newKey;
    }

    public static void ec2Try(){
        Ec2Client ec2 = Ec2Client.create();
        String amiId = "ami-04ad2567c9e3d7893";
        String javaInstallation = "wget --no-check-certificate --no-cookies --header \"Cookie: oraclelicense=accept-securebackup-cookie\" http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.rpm\n"+
                "sudo yum install -y jdk-8u141-linux-x64.rpm\n";

//        ami without java - ami-04ad2567c9e3d7893 (T2_MICRO)
//        ami with java - ami-00e95a9222311e8ed (T2_MICRO)

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .keyName("key1")
                .securityGroupIds("sg-0f83f8c78a44f97a6")
                .userData(Base64.getEncoder().encodeToString(
                                ("#!/bin/bash\n"+
                                         javaInstallation).getBytes()))
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

    public static SqsClient createSQS() {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();


        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(QUEUE_NAME)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }


        return sqs;
    }
    public static void sendMessage(SqsClient sqs, String message){
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build();
        sqs.sendMessage(send_msg_request);


//        // Send multiple messages to the queue
//        SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
//                .queueUrl(queueUrl)
//                .entries(
//                        SendMessageBatchRequestEntry.builder()
//                                .messageBody("Hello from message 1 - Ohad")
//                                .id("msg_1")
//                                .build()
//                        ,
//                        SendMessageBatchRequestEntry.builder()
//                                .messageBody("Hello from message 2 - Ori")
//                                .id("msg_2")
//                                .build())
//                .build();
//        sqs.sendMessageBatch(send_batch_request);
//
//        // receive messages from the queue
//        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
//                .queueUrl(queueUrl)
//                .build();
//        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
//        System.out.println(messages.toString());
//        receiveRequest = ReceiveMessageRequest.builder()
//                .queueUrl(queueUrl)
//                .build();
//         messages = sqs.receiveMessage(receiveRequest).messages();
//        System.out.println(messages.toString());
//        receiveRequest = ReceiveMessageRequest.builder()
//                .queueUrl(queueUrl)
//                .build();
//        messages = sqs.receiveMessage(receiveRequest).messages();
//        System.out.println(messages.toString());
//        for (Message m : messages) {
//            System.out.println(m.body());
//        }

//         delete messages from the queue
//        for (Message m : messages) {
//            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
//                    .queueUrl(queueUrl)
//                    .receiptHandle(m.receiptHandle())
//                    .build();
//            sqs.deleteMessage(deleteRequest);
//        }
    }

    public static void waitForMessage(String queueUrl){
        System.out.println("waiting for a message");
        while (true)
        {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            //System.out.println(messages.size());
            if(!messages.isEmpty())
                for(Message message:messages)
                {
                    String path= message.body();
                    String[] split=path.split("/",4);
                    s3.getObject(GetObjectRequest.builder().bucket(split[2]).key(split[3]).build(),ResponseTransformer.toFile(new File("./output/output.html")));
                    System.out.println("message received!");
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(message.receiptHandle()).build();
                    sqs.deleteMessage(deleteMessageRequest);
                    return;
                }
        }
    }

}

