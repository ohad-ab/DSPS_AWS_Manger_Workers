
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
public class Manager {
    private static boolean terminate = false;
    private LinkedBlockingQueue<InFile> queue;
    public static void main(String[] args) {
        System.out.println("ff");
        sqsTry();

//        LinkedBlockingQueue<InFile> queue = new LinkedBlockingQueue<>();
//        String line;
//        while ((line = br.readLine()) != null) {
//            String[] splitted = line.split("\t");
//            InFile f = new InFile(splitted[0], splitted[1]);
//            queue.add(f);
//        }
    }
    public static String generate_keyName(){//TODO: check if needed.
        String newKey = "inputTest";
        //TODO: Get available name
        return  newKey;
    }
    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();

    public static void sqsTry() {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);

            String queueUrl = listQueuesResponse.queueUrls().get(0);
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("s3://dsps-221/pdf.html")
                .build();
            sqs.sendMessage(send_msg_request);
            while (!terminate)
            {

                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
                Message message = messages.get(0);
                    String body= message.body();
                    if(body.equals("terminate"))
                    {
                        System.out.println("ee");
                        terminate = true;
                        Ec2Client ec2 = Ec2Client.create();
                        List<Reservation> reservations = ec2.describeInstances().reservations();
                        for (Reservation reservation:reservations) {
                            Instance instance = reservation.instances().get(0);
                            if(!instance.tags().isEmpty() && instance.tags().get(0).value().equals("Manager") && (instance.state().nameAsString().equals("running")))
                                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
                        }
                    }



            }

        }
        catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
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
    }
}
