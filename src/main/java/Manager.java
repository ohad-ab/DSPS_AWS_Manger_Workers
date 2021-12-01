import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.util.*;

public class Manager {
    private static boolean terminate = false;
    public static int activeWorker = 0;
    public static int workerCount = 0;
    public static String bucket = "dsps-221";
    public static String localManagerSQSurl = "https://sqs.us-east-1.amazonaws.com/150025664389/LOCAL-MANAGER";


    public static void main(String[] args) {
        System.out.println("Manager Started");
        waitForMessages(localManagerSQSurl);



//        String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/445821044214/requests_queue"; //Ori
//        String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/445821044214/answers_queue"; //Ori

//        String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/150025664389/requestsSqs"; //Ohad
//        String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/150025664389/answersSqs"; //Ohad
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
                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(localManagerSQSurl).messageAttributeNames("Output").visibilityTimeout(0).build();
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
                if (!messages.isEmpty()) {
                    Message message = messages.get(0);
                    String body = message.body();
                    if (body.equals("terminate")) {
                        terminate = true;
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(localManagerSQSurl).receiptHandle(message.receiptHandle()).build();
                        DeleteMessageResponse response= sqs.deleteMessage(deleteMessageRequest);
                        System.out.println("terminate");
                        processTerminate("Worker");
                        processTerminate("Manager");
                    } else if(!message.messageAttributes().containsKey("Output")) {
                        System.out.println("input received "+message.body());
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(localManagerSQSurl).receiptHandle(message.receiptHandle()).build();
                        DeleteMessageResponse response= sqs.deleteMessage(deleteMessageRequest);
                        System.out.println(response.toString());
                        Request request = new Request(body);
                        Thread thread = new Thread(request);
                        thread.start();
                        //thread.join();
                    }
                }


            }

        } catch (SqsException /*| InterruptedException*/ e) {
            //System.err.println(e.awsErrorDetails().errorMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private  static  void processTerminate(String name) {
        Ec2Client ec2 = Ec2Client.create();
        List<Reservation> reservations = ec2.describeInstances().reservations();
        for (Reservation reservation : reservations) {
            Instance instance = reservation.instances().get(0);
            if (!instance.tags().isEmpty()) {
                if((instance.tags().get(1).value().equals(name)|| instance.tags().get(0).value().equals(name)) && (instance.state().nameAsString().equals("running"))) {
                    ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
                    System.out.println("terminate "+name);
                }
            }

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
                                "aws s3 cp s3://"+bucket+"/Worker.jar Worker.jar\n" + //Ori S3
                                "java -jar Worker.jar " + requestsSQS + " " + answersSQS + "\n"
                        ).getBytes()))
                .build();
//        String reservation_id = ec2.describeInstances().reservations().get(0).instances().get(0).instanceId();
        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        ec2.createTags(CreateTagsRequest.builder().resources(instanceId).tags(Tag.builder()
                .key("Name").value("Worker_" + workerCount++).build(), Tag.builder().key("Type").value("Worker").build()).build());


//        List<Reservation> reservList = ec2.describeInstances().reservations();
//        List<Instance> instanceList =  reservList.get(0).instances();
        //RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();
        System.out.println(instances);
    }
    private static File createOutput(List<String>messages)
    {
        String startOfHTML = "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>PDF Converter</title>\n" +
                "    <style>\n" +
                "        table.unstyledTable {\n" +
                "            border: 1px solid #000000;\n" +
                "        }\n" +
                "\n" +
                "        table.unstyledTable td,\n" +
                "        table.unstyledTable th {\n" +
                "            border: 1px solid #AAAAAA;\n" +
                "        }\n" +
                "\n" +
                "        table.unstyledTable thead {\n" +
                "            background: #DDDDDD;\n" +
                "        }\n" +
                "\n" +
                "        table.unstyledTable thead th {\n" +
                "            font-weight: normal;\n" +
                "            height: max-content;\n" +
                "            width: max-content;\n" +
                "        }\n" +
                "\n" +
                "    </style>\n" +
                "</head>\n" +
                "\n" +
                "<body>\n" +
                "    <h1 style=\"color: #5e9ca0;\">Ohad and Ori PDF converter&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;</h1>\n" +
                "    <h2 style=\"color: #2e6c80;\">Output:</h2>\n" +
                "    <table class=\"unstyledTable\">\n" +
                "        <thead>\n" +
                "            <tr>\n" +
                "                <th><strong>Operation</strong></th>\n" +
                "                <th style=\"height: 18px; width: 221px;\"><strong>Input File</strong></th>\n" +
                "                <th style=\"height: 18px; width: 179px;\"><strong>Output File</strong></th>\n" +
                "            </tr>\n" +
                "        </thead>\n" +
                "        <tbody>";
        String endOfHTML = "</tbody>\n" +
                "    </table>\n" +
                "</body>\n" +
                "</html>";
        String addedRows = "";
        PrintWriter outHTML = null;
        try {
          outHTML = new PrintWriter("./output.html");
          for(String message: messages)
            //  addedRows.concat(generateHTMLTableRow(message));
              addedRows=addedRows+generateHTMLTableRow(message);
          outHTML.println(startOfHTML + addedRows + endOfHTML);
          outHTML.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return new File("./output.html");
    }
public static String generateHTMLTableRow(String message){
        String[] splittedMessage = message.split("\t");
        String originalUrl = splittedMessage[0];
        String[] splittedOriginUrl = originalUrl.split("/");
        String newUrl = splittedMessage[1];
        String[] splittedNewUrl = newUrl.split(".s3.amazonaws.com/");
        String operation = splittedMessage[2];
        String originalName = splittedOriginUrl[splittedOriginUrl.length-1].split(".pdf")[0];
        String newName = splittedNewUrl[1];

        return "\n<tr>\n" +
        "<td>"+ operation + "</td>\n" +
        "<td> <a href="+ originalUrl +">" + originalName +"</a></td>\n" +
        "<td><a href="+ newUrl + ">" + newName + "</a></td>\n" +
        "</tr>\n";
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
        private String localMessage;

        public Request(String localMessage) {
            this.localMessage = localMessage;
        }

        @Override
        public void run() {
            try {
                System.out.println("starting thread "+localMessage);
                String[] splitMessage = localMessage.split("\t");
                String[] splitURL = splitMessage[0].split("/");
                int ratio = Integer.parseInt(splitMessage[1]);
                S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
                InputStream input = s3.getObject(GetObjectRequest.builder().bucket(splitURL[2]).key(splitURL[3]).build(), ResponseTransformer.toInputStream());
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                ArrayList<String> messagesToWorkers = new ArrayList<>();
                int messagecount = 0;
                String line;
                while ((line = reader.readLine()) !=null) {
                    messagesToWorkers.add(line);
                    messagecount++;
                }
                SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
//                String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/445821044214/requests_queue"; //Ori
//                String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/445821044214/answers_queue"; //Ori

                        String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/150025664389/requestsSqs"; //Ohad
                        String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/150025664389/answersSqs"; //Ohad
                SendMessageRequest send_msg_request;
                System.out.println("sending messages to workers");
                for (String messagesToWorker : messagesToWorkers)
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(requestsSqsUrl).messageBody(messagesToWorker).build());
                int numOfWorkers = messagecount / ratio + (messagecount % ratio != 0 ? 1 : 0);
                if(numOfWorkers >15)
                    numOfWorkers = 15;
                for (int i = 0; i < numOfWorkers; i++)
                    runNewWorker(requestsSqsUrl, answersSqsUr);

                ArrayList<String> messagesToManager = new ArrayList<>();

                while (true)
                {
                    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(answersSqsUr).build();
                    List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
                    //System.out.println(messages.size());
                    if(!messages.isEmpty())
                        for(Message message:messages)
                        {
                            messagesToManager.add(message.body());
                            System.out.println("message received!");
                            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(answersSqsUr).receiptHandle(message.receiptHandle()).build();
                            sqs.deleteMessage(deleteMessageRequest);
                            messagecount--;
                        }
                    if(messagecount == 0)
                        break;
                }
                File output = createOutput(messagesToManager);
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key("output").build(), RequestBody.fromFile(output));
                final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                messageAttributes.put("Output",MessageAttributeValue.builder().dataType("String").stringValue("v").build());
                send_msg_request=SendMessageRequest.builder().queueUrl(localManagerSQSurl).messageBody("s3://"+bucket+"/"+"output")
                        .messageAttributes(messageAttributes).build();
                sqs.sendMessage(send_msg_request);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}