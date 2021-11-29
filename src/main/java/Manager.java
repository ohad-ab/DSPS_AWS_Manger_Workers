
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class Manager {
    private static boolean terminate = false;
    public static int activeWorker = 0;
    private LinkedBlockingQueue<InFile> queue;
    public static int workerCount = 0;

    public static void main(String[] args) {
        System.out.println("Manager Started");
        String localManagerSQSurl = "https://sqs.us-east-1.amazonaws.com/150025664389/LOCAL-MANAGER";
        waitForMessages(localManagerSQSurl);

//        String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/445821044214/requests_queue"; //Ori
//        String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/445821044214/answers_queue"; //Ori

//        String requestsSqsUrl = ""; //Ohad
//        String answersSqsUr = ""; //Ohad
//        runNewWorker(requestsSqsUrl, answersSqsUr);


//        LinkedBlockingQueue<InFile> queue = new LinkedBlockingQueue<>();
//        String line;
//        while ((line = br.readLine()) != null) {
//            String[] splitted = line.split("\t");
//            InFile f = new InFile(splitted[0], splitted[1]);
//            queue.add(f);
//        }
    }

    public static String generate_keyName() {//TODO: check if needed.
        String newKey = "inputTest";
        //TODO: Get available name
        return newKey;
    }

    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();

    public static void waitForMessages(String localManagerSQSurl) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        try {
//            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
//            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
//
//            String queueUrl = listQueuesResponse.queueUrls().get(0);
//            SendMessageRequest send_msg_request = SendMessageRequest.builder()
//                    .queueUrl(queueUrl)
//                    .messageBody("s3://dsps-221/pdf.html")
//                    .build();
//            sqs.sendMessage(send_msg_request);
            while (!terminate) {
                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(localManagerSQSurl).build();
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
                if (!messages.isEmpty()) {
                    Message message = messages.get(0);
                    String body = message.body();
                    if (body.equals("terminate")) {
                        terminate = true;
                        Ec2Client ec2 = Ec2Client.create();
                        List<Reservation> reservations = ec2.describeInstances().reservations();
                        for (Reservation reservation : reservations) {
                            Instance instance = reservation.instances().get(0);
                            if (!instance.tags().isEmpty() && instance.tags().get(0).value().equals("test") && (instance.state().nameAsString().equals("running"))) {
                                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
                            }
                        }
                    } else {
                        Request request = new Request(body);
                        Thread thread = new Thread(request);
                        thread.run();
                    }
                }


            }

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


    private static void runNewWorker(String requestsSQS, String answersSQS) {
        Ec2Client ec2 = Ec2Client.create();
        System.out.println("Running new worker #" + workerCount);
        String amiId = "ami-00e95a9222311e8ed";
        String fileSystemInstallation = "#cloud-config\n" +
                "package_update: true\n" +
                "package_upgrade: true\n" +
                "runcmd:\n" +
                "- yum install -y amazon-efs-utils\n" +
                "- apt-get -y install amazon-efs-utils\n" +
                "- yum install -y nfs-utils\n" +
                "- apt-get -y install nfs-common\n" +
                "- file_system_id_1=fs-0cdb88b94a7ee1ded\n" +
                "- efs_mount_point_1=/mnt/efs/fs1\n" +
                "- mkdir -p \"${efs_mount_point_1}\"\n" +
                "- test -f \"/sbin/mount.efs\" && printf \"\\n${file_system_id_1}:/ ${efs_mount_point_1} efs iam,tls,_netdev\\n\" >> /etc/fstab || printf \"\\n${file_system_id_1}.efs.us-east-1.amazonaws.com:/ ${efs_mount_point_1} nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport,_netdev 0 0\\n\" >> /etc/fstab\n" +
                "- test -f \"/sbin/mount.efs\" && grep -ozP 'client-info]\\nsource' '/etc/amazon/efs/efs-utils.conf'; if [[ $? == 1 ]]; then printf \"\\n[client-info]\\nsource=liw\\n\" >> /etc/amazon/efs/efs-utils.conf; fi;\n" +
                "- retryCnt=15; waitTime=30; while true; do mount -a -t efs,nfs4 defaults; if [ $? = 0 ] || [ $retryCnt -lt 1 ]; then echo File system mounted successfully; break; fi; echo File system not available, retrying to mount.; ((retryCnt--)); sleep $waitTime; done;\n";
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
                        ("#!/bin/bash\n" +
                                fileSystemInstallation +
                                //       workerJar
                                "aws s3 cp s3://oo-dspsp-ass1/Worker.jar Worker.jar\n" + //Ori S3
                                "java -jar Worker.jar " + requestsSQS + " " + answersSQS + "\n"
                        ).getBytes()))
                .build();
//        String reservation_id = ec2.describeInstances().reservations().get(0).instances().get(0).instanceId();
        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        ec2.createTags(CreateTagsRequest.builder().resources(instanceId).tags(Tag.builder()
                .key("Name").value("Worker_" + workerCount++).build()).build());


//        List<Reservation> reservList = ec2.describeInstances().reservations();
//        List<Instance> instanceList =  reservList.get(0).instances();
        //RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();
        System.out.println(instances);
    }


//        try {
//            CreateQueueRequest request = CreateQueueRequest.builder()
//                    .queueName(QUEUE_NAME)
//                    .build();
//            CreateQueueResponse create_result = sqs.createQueue(request);
//        } catch (QueueNameExistsException e) {
//            throw e;
//
//        }
//
//        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
//                .queueName(QUEUE_NAME)
//                .build();
//        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
//
//        SendMessageRequest send_msg_request = SendMessageRequest.builder()
//                .queueUrl(queueUrl)
//                .messageBody("s3://dsps-221/pdf.html")
//                .build();
//        sqs.sendMessage(send_msg_request);


    static class Request implements Runnable {
        private String mesaage;

        public Request(String message) {
            this.mesaage = message;
        }

        @Override
        public void run() {
            try {
                String[] splitMessage = mesaage.split("\t");
                String[] splitURL = splitMessage[0].split("/");
                int ratio = Integer.parseInt(splitMessage[1]);
                S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
                InputStream input = s3.getObject(GetObjectRequest.builder().bucket(splitURL[2]).key(splitURL[3]).build(), ResponseTransformer.toInputStream());
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                ArrayList<String> messagesToWorkers = new ArrayList<>();
                int messagecount = 0;
                while (reader.ready()) {
                    messagesToWorkers.add(reader.readLine());
                    messagecount++;
                }
                SqsClient workerSQS = SqsClient.builder().region(Region.US_EAST_1).build();
                        String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/445821044214/requests_queue"; //Ori
                        String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/445821044214/answers_queue"; //Ori

                //        String requestsSqsUrl = ""; //Ohad
                //        String answersSqsUr = ""; //Ohad
                SendMessageRequest send_msg_request;
                for (String messagesToWorker : messagesToWorkers)
                    SendMessageRequest.builder().queueUrl(requestsSqsUrl).messageBody(messagesToWorker).build();
                int numOfWorkers = messagecount/ratio + (messagecount%ratio != 0?1:0);
                for(int i = 0;i<numOfWorkers; i++)
                    runNewWorker(requestsSqsUrl, answersSqsUr);

            } catch (Exception e) {
            }
        }
    }
}