import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.xml.parsers.ParserConfigurationException;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Worker {
    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
//    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
    private static final SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    //       private static String bucket_name = "oo-dspsp-ass1";
    private static String bucket_name = "dsps-221";

    public static void main(String[] args){
//        Just an idea, we don't have to do it this way
        String requestsSqs = args[0];
        String answersSqs = args[1];
        //Ori
        //requests SQS - https://sqs.us-east-1.amazonaws.com/445821044214/requests_queue
        //answers SQS - https://sqs.us-east-1.amazonaws.com/445821044214/answers_queue
        while (true) {
            Message receivedMessage = waitForMessage(requestsSqs);
            String[] splittedMessage = receivedMessage.body().split("\t");
            String operation = splittedMessage[0];
            String appName = receivedMessage.messageAttributes().get("Name").stringValue();
            boolean isMessageSent = false;
            try {
                URL url = new URL(splittedMessage[1]);
                String keyName = generate_keyName(splittedMessage[1]);
                File localFile;
                String outputMessage;
                localFile = handleOperation(operation, url, keyName);
                outputMessage = url + "\t" + uploadFileToS3(localFile, "changed/"+keyName) + "\t" + operation;
                isMessageSent = sendMessage(appName, answersSqs, outputMessage);
                localFile.delete();

            } catch (Exception e) {
                String errorMessage = e.getMessage();
                System.err.println(errorMessage);
                String outputMessage = splittedMessage[1] + "\tException\t" + errorMessage + "\t" + operation;;
               isMessageSent = sendMessage(appName, answersSqs, outputMessage);
            }
            finally {
                if(isMessageSent)
                    deleteMessage(receivedMessage,requestsSqs);
            }

        }
    }

    public static File handleOperation(String operation, URL url, String keyName) throws IOException, ParserConfigurationException, ProblemInProcessException {

        switch (operation) {
            case "ToImage":
                return new File(toImage(url, keyName));
            case "ToHTML":
                return new File(toHTML(url, keyName));
            case "ToText":
                return new File(toText(url, keyName));
            default:
                System.err.println("Operation not defined");
                throw new ProblemInProcessException("Wrong operation");
        }
    }


    public static Message waitForMessage(String sqsurl) {
        Boolean gotMessage = false;
        String body = null;
        Message message = null;
        while (!gotMessage) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(sqsurl).messageAttributeNames("Name").maxNumberOfMessages(1).build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                message = messages.get(0);
                body = message.body();
                System.out.println("message received:\n" + body);
                gotMessage = true;
            }
        }
        return message;
    }

    public static void deleteMessage(Message m, String sqsurl){
        try{
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(sqsurl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);
        } catch (AwsServiceException | SdkClientException e) {
            e.printStackTrace();
        }
    }


    public static String toImage(URL url, String keyName) throws IOException {
        InputStream is = url.openStream();

        PDDocument document = PDDocument.load(is);
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        int pageCounter = 0;
        BufferedImage bim = pdfRenderer.renderImageWithDPI(pageCounter, 300, ImageType.RGB);
        ImageIOUtil.writeImage(bim, "./" + keyName + ".png", 300);
        document.close();
        is.close();
        return "./" + keyName + ".png";
    }

    public static String toHTML(URL url, String keyName) throws IOException, ParserConfigurationException {
        InputStream is = url.openStream();
        PDDocument pdf = PDDocument.load(is);
        PDDocument page = new PDDocument();
        page.addPage(pdf.getPage(0));
        Writer output = new PrintWriter("./"+keyName+".html", "utf-8");
        new PDFDomTree().writeText(page, output);
        output.close();
        page.close();
        pdf.close();
        is.close();
        return "./"+keyName+".html";


    }

    public static String toText(URL url, String keyName) throws IOException {
        InputStream is = url.openStream();
        PDDocument document = PDDocument.load(is);
        PDDocument page = new PDDocument();
        page.addPage(document.getPage(0));
        PDFTextStripper textStripper = new PDFTextStripper();
        String str = textStripper.getText(page);
        PrintWriter outTxt = new PrintWriter("./"+ keyName + ".txt","utf-8");
        outTxt.println(str);
        System.out.println(str);
        outTxt.close();
        document.close();
        page.close();
        is.close();
        return "./"+ keyName +".txt";
    }

    public static boolean sendMessage(String appName, String queueUrl, String message) {
        try {
            final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            messageAttributes.put("Name",MessageAttributeValue.builder().dataType("String").stringValue(appName).build());
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                            .messageAttributes(messageAttributes)
                    .build());
            System.out.println("\nmessage sent:\n" + message);
            return true;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return false;
        }
    }

    public static String uploadFileToS3(File localFile, String key_name){
        // Get s3


        // Upload input to S3
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket_name)
                .key(key_name)
                .acl(ObjectCannedACL.PUBLIC_READ) //Access control list
                .build(), RequestBody.fromFile(localFile));
        String newUrl = "https://"+bucket_name+".s3.amazonaws.com/"+key_name;
        System.out.println("\nfile uploaded to:\n" +newUrl );

        return newUrl;
    }

    public static String generate_keyName(String path){
        String[] splitted = path.split("/");
        String newKey = splitted[splitted.length-1].split(".pdf")[0] + "_" + new Date().getTime();
        return  newKey;
    }
}
class ProblemInProcessException extends Exception
{
    // Parameterless Constructor
    public ProblemInProcessException() {}

    // Constructor that accepts a message
    public ProblemInProcessException(String message)
    {
        super(message);
    }
}