
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
public class Manager {

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
                .build();
        sqs.sendMessage(send_msg_request);
    }
}
