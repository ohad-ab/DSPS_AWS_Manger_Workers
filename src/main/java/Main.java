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
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
//import java.util.Date;
//import java.util.List;
import java.io.File;
//import java.util.Base64;
import java.util.*;


//import InFile;


public class Main {
    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
    private static final String APP_NAME = "Local_" + new Date().getTime();
    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
    private static final SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();//createSQS();
    private static final String localManagerSQSurl = "https://sqs.us-east-1.amazonaws.com/150025664389/LOCAL-MANAGER";
    private static final String managerLocalSQSurl = "https://sqs.us-east-1.amazonaws.com/150025664389/MANAGER-LOCAL";
    private static boolean terminate=true;

    public static void main(String[] args) throws Exception {
        // Get data from args
        File input = new File(args[0]);
        String output = args[1];
        int workerRatio = Integer.parseInt(args[2]);
        Boolean terminate = args.length == 4 && args[3].equals("terminate"); //TODO: Check with Moshe, What is 'terminate' type?

        // Get s3
//        String bucket_name = "oo-dspsp-ass1";
        String bucket_name = "dsps-221";

        // Upload input to S3
        String key_name = generate_keyName();
        s3.putObject(PutObjectRequest.builder().bucket(bucket_name).key(key_name).build(), RequestBody.fromFile(input));

        //send input to Local-Manager sqs
        sendMessage("s3://"+bucket_name+"/"+key_name+"\t"+workerRatio);
        String managerJarLoc ="s3://"+bucket_name+"/Manager.jar";
        Ec2Client ec2 = Ec2Client.create();
        if (!isRunningEc2(ec2)){
            runNewManager(ec2,managerJarLoc);
        }

        waitForMessage(output);
        if(terminate)
        {
            sendMessage("terminate");
        }

    }

    private static void runNewManager(Ec2Client ec2, String managerJar) {
        System.out.println("Running new manager");
        String amiId = "ami-00e95a9222311e8ed";
//        String javaInstallation = "wget --no-check-certificate --no-cookies --header \"Cookie: oraclelicense=accept-securebackup-cookie\" http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.rpm\n"+
//                "sudo yum install -y jdk-8u141-linux-x64.rpm\n";
        //sg-0a245fb00956df9ba security group ohad
        //sg-0f83f8c78a44f97a6 security group ori
        IamInstanceProfileSpecification iam = IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(iam)
                .userData(Base64.getEncoder().encodeToString(
                        ("#!/bin/bash\n"+
                                //javaInstallation+
                                "aws s3 cp "+managerJar+" Manager.jar\n" +
                                "java -jar Manager.jar\n"
                                ).getBytes()))
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        //create tags
        String instanceId = response.instances().get(0).instanceId();
        ec2.createTags(CreateTagsRequest.builder().resources(instanceId).tags(Tag.builder()
                .key("Name").value("Manager").build(), Tag.builder().key("Type").value("Manager").build()).build());


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
        String newKey = "input_"+APP_NAME;
        //TODO: Get available name
        return  newKey;
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
    public static void sendMessage(String message){
        //String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("Name",MessageAttributeValue.builder().dataType("String").stringValue(APP_NAME).build());
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(localManagerSQSurl)
                .messageBody(message)
                .messageAttributes(messageAttributes)
                .build();
        sqs.sendMessage(send_msg_request);


    }

    public static void waitForMessage(String outputPath){
        System.out.println("waiting for a message");
        while (true)
        {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(managerLocalSQSurl).messageAttributeNames("Name").build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if(!messages.isEmpty())
                for(Message message:messages)
                {
                    if(message.messageAttributes().containsValue(APP_NAME))
                    {
                        String path = message.body();
                        String[] split = path.split("/");
                        s3.getObject(GetObjectRequest.builder().bucket(split[2]).key(split[3]).build(), ResponseTransformer.toFile(new File(outputPath)));
                        System.out.println("message received!");
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(managerLocalSQSurl).receiptHandle(message.receiptHandle()).build();
                        sqs.deleteMessage(deleteMessageRequest);
                        return;
                    }
                }
        }
    }

}

