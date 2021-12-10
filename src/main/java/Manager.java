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
//import sun.nio.ch.ThreadPool;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Manager {
    private static boolean terminate = false;
    public static int activeWorker = 0;
    public static int workerCount = 0;
    public static String bucket = "dsps-221"; //Ohad
//    public static String bucket = "oo-dspsp-ass1"; //Ori
    public static String localManagerSQSurl = "https://sqs.us-east-1.amazonaws.com/150025664389/LOCAL-MANAGER";//ohad
    private static final String managerLocalSQSurl = "https://sqs.us-east-1.amazonaws.com/150025664389/MANAGER-LOCAL";//ohad
//        public static String localManagerSQSurl = "https://sqs.us-east-1.amazonaws.com/445821044214/Local-Manager"; //ori
//        private static final String managerLocalSQSurl = "https://sqs.us-east-1.amazonaws.com/445821044214/Manager-Local"; //ori
//   private static String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/445821044214/requests_queue"; //Ori
//    private static String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/445821044214/answers_queue"; //Ori

    private static String requestsSqsUrl = "https://sqs.us-east-1.amazonaws.com/150025664389/requestsSqs"; //Ohad
    private static String answersSqsUr = "https://sqs.us-east-1.amazonaws.com/150025664389/answersSqs"; //Ohad
    public static Integer numOfWorkers = 0;
    public static ExecutorService pool = Executors.newFixedThreadPool(10);

    public static void main(String[] args) {
        System.out.println("Manager Started");
        waitForMessages();
    }

    public static void waitForMessages() {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        try {
            while (!terminate) {
                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(localManagerSQSurl).messageAttributeNames("Name").visibilityTimeout(0).build();
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
                if (!messages.isEmpty()) {
                    for(Message message: messages){
                        String body = message.body();
                        if (body.equals("terminate")) {
                            terminate = true;
                            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(localManagerSQSurl).receiptHandle(message.receiptHandle()).build();
                            sqs.deleteMessage(deleteMessageRequest);
                            pool.shutdown();
                            while(!pool.awaitTermination(30, TimeUnit.SECONDS)){}//waiting for all the threads to end
                            sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(answersSqsUr).build());
                            sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(requestsSqsUrl).build());
                            System.out.println("terminate");
                            processTerminate("Worker");
                            processTerminate("Manager");
                        }
                        else {
                            System.out.println("input received "+message.body());
                            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(localManagerSQSurl).receiptHandle(message.receiptHandle()).build();
                            DeleteMessageResponse response= sqs.deleteMessage(deleteMessageRequest);
                            System.out.println(response.toString());
                            Request request = new Request(message);
                            pool.execute(request);
                        }
                    }
                }
            }
        } catch (SqsException | InterruptedException /*| InterruptedException*/ e) {
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
                        ("#!/bin/bash\n" +
                                "mkdir files\n"+
                                "aws s3 cp s3://"+bucket+"/Worker.jar ./files/Worker.jar\n" +
                                "java -jar ./files/Worker.jar " + requestsSQS + " " + answersSQS + "\n"
                        ).getBytes()))
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        addTagToInstance(ec2, instanceId);

        List<Instance> instances = response.instances();
        System.out.println(instances);
    }

    private static void addTagToInstance(Ec2Client ec2, String instanceId) {
        try {
            ec2.createTags(CreateTagsRequest.builder().resources(instanceId).tags(Tag.builder()
                    .key("Name").value("Worker_" + workerCount++).build(), Tag.builder().key("Type").value("Worker").build()).build());
        }
        catch  (Exception e){
            try {
                System.err.println("Problem with creating tag, trying again");
                TimeUnit.SECONDS.sleep(1);
                addTagToInstance(ec2, instanceId);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
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
                "    <h1 style=\"color: #5e9ca0;\">Ohad and Ori - PDF converter&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;</h1>\n" +
                "    <h2 style=\"color: #2e6c80;\">Output:</h2>\n" +
                "    <table class=\"unstyledTable\">\n" +
                "        <thead>\n" +
                "            <tr>\n" +
                "                <th><strong>#</strong></th>\n" +
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
          int index = 1;
          for(String message: messages){
              addedRows = addedRows + generateHTMLTableRow(message, index);
              index++;
          }
          outHTML.println(startOfHTML + addedRows + endOfHTML);
          outHTML.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return new File("./output.html");
    }
public static String generateHTMLTableRow(String message, int i){
        String[] splittedMessage = message.split("\t");
        String originalUrl = splittedMessage[0];
        String[] splittedOriginUrl = originalUrl.split("/");
        String newUrl;
        String newName;
        String finalOutput;
        String operation;

        if (splittedMessage[1].split("\t")[0].equals("Exception")){
            newUrl ="<span style=\"color: red;\">" + splittedMessage[2]+ "</span>";
            newName = "";
            operation = splittedMessage[3];
        }
        else {
            newUrl = "<a href=" + splittedMessage[1] + ">";
            String[] splittedNewUrl = newUrl.split(".s3.amazonaws.com/");
            newName = splittedNewUrl[1] + "</a>";
            operation = splittedMessage[2];

        }
        String originalName = splittedOriginUrl[splittedOriginUrl.length - 1].split(".pdf")[0];
        finalOutput = newUrl + newName;
        return "\n<tr>\n" +
                "<td>"+ i + "</td>\n" +
        "<td>"+ operation + "</td>\n" +
        "<td> <a href="+ originalUrl +">" + originalName +"</a></td>\n" +
        "<td>" + finalOutput + "</td>\n" +
        "</tr>\n";
    }


    static class Request implements Runnable {
        private Message localMessage;

        public Request(Message localMessage) {
            this.localMessage = localMessage;
        }

        @Override
        public void run() {
            try {
                System.out.println("starting thread "+localMessage.body());
                String appName = localMessage.messageAttributes().get("Name").stringValue();
                String[] splitMessage = localMessage.body().split("\t");
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

                SendMessageRequest send_msg_request;
                Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                messageAttributes.put("Name",MessageAttributeValue.builder().dataType("String").stringValue(appName).build());
                System.out.println("sending "+messagesToWorkers.size()+" messages to workers");
                for (String messagesToWorker : messagesToWorkers)
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(requestsSqsUrl).messageBody(messagesToWorker)
                            .messageAttributes(messageAttributes).build());
                int numOfNeededWorkers;
                synchronized (numOfWorkers) {
                    numOfNeededWorkers = (int) (Math.ceil((double) messagecount / ratio) - numOfWorkers);

                    if (numOfWorkers + numOfNeededWorkers > 15)
                        numOfNeededWorkers = 15 - numOfWorkers;

                    for (int i = 0; i < numOfNeededWorkers; i++) {
                        runNewWorker(requestsSqsUrl, answersSqsUr);
                        numOfWorkers++;
                    }
                }
                ArrayList<String> messagesToManager = new ArrayList<>();

                while (true)
                {
                    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(answersSqsUr).messageAttributeNames("Name").build();
                    List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
                    if(!messages.isEmpty())
                        for(Message message:messages)
                        {
                            if(message.messageAttributes().get("Name").stringValue().equals(appName))
                            {
                                messagesToManager.add(message.body());
                                System.out.println("message received!");
                                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(answersSqsUr).receiptHandle(message.receiptHandle()).build();
                                sqs.deleteMessage(deleteMessageRequest);
                                messagecount--;
                            }
                        }
                    if(messagecount == 0)
                        break;
                }
                System.out.println("received "+messagesToManager.size()+" messages");
                File output = createOutput(messagesToManager);
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key("output"+appName).build(), RequestBody.fromFile(output));
                messageAttributes = new HashMap<>();
                messageAttributes.put("Name",MessageAttributeValue.builder().dataType("String").stringValue(appName).build());
                send_msg_request=SendMessageRequest.builder().queueUrl(managerLocalSQSurl).messageBody("s3://"+bucket+"/"+"output"+appName)
                        .messageAttributes(messageAttributes).build();
                sqs.sendMessage(send_msg_request);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}