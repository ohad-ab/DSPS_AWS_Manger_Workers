import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
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
import java.util.List;

public class Worker {
    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
    private static final SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();



    public static void main(String[] args) throws IOException, ParserConfigurationException {
//        Just an idea, we don't have to do it this way
        String requestsSqs = args[0];
        String answersSqs = args[1];
        //Ori
        //requests SQS - https://sqs.us-east-1.amazonaws.com/445821044214/requests_queue
        //answers SQS - https://sqs.us-east-1.amazonaws.com/445821044214/answers_queue
        while (true) {
            String[] receivedMessage = waitForMessage(requestsSqs).split("\t");
            String operation = receivedMessage[0];
            URL url = new URL(receivedMessage[1]); //TODO: catch exeptions
            System.out.println("----check-----\n" +url);
            String keyName = generate_keyName(receivedMessage[1]);
            File localFile;
            String outputMessage;
            localFile = handleOperation(operation, url, keyName);
            if (localFile != null) {
                outputMessage = url + uploadFileToS3(localFile, keyName) + "\t" + operation;
                sendMessage(answersSqs, outputMessage);
                localFile.delete();
            } else {
                System.err.println("Problem with local file");
            }
        }
    }

    public static File handleOperation(String operation, URL url, String keyName) throws IOException, ParserConfigurationException {

        switch (operation) { //TODO: catch exeptions
            case "ToImage":
                return new File(toImage(url, keyName));
            case "ToHTML":
                return new File(toHTML(url, keyName));
            case "ToText":
                return new File(toText(url, keyName));
            default:
                System.err.println("Operation not defined"); //TODO: throw exception?
        }
        return null;
    }


    public static String waitForMessage(String sqsurl) {
        Boolean gotMessage = false;
        String body = null;
        while (!gotMessage) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(sqsurl).build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                body = message.body();
                System.out.println("message received:\n" + body);
                deleteMessage(message,sqsurl);
                gotMessage = true;
            }
        }
        return body;
    }

    public static void deleteMessage(Message m, String sqsurl){ //TODO: handle eceptions
        try{
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(sqsurl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);
        } catch (AwsServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }

//    TODO: check if it's ok to return the local path (maybe we need ebs?)

    //    TODO: can we assume that it's only one image?
    public static String toImage(URL url, String keyName) throws IOException {
        InputStream is = url.openStream();

        PDDocument document = PDDocument.load(is);
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        int pageCounter = 0;
//        PDPage page = document.getPage(0);
//        for (PDPage page : document.getPages()) {
            // note that the page number parameter is zero based
            BufferedImage bim = pdfRenderer.renderImageWithDPI(pageCounter, 300, ImageType.RGB);

            // suffix in filename will be used as the file format
//            ImageIOUtil.writeImage(bim, "./output/Images/" + keyName + "-" + (++pageCounter) + ".png", 300);
            ImageIOUtil.writeImage(bim, "./" + keyName + ".png", 300);
//        }
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
        PrintWriter outTxt = new PrintWriter("./"+ keyName + ".txt");
        outTxt.println(str);
        System.out.println(str);
        outTxt.close();
        document.close();
        page.close();
        is.close();
        return "./"+ keyName +".txt";
    }

    public static void sendMessage(String queueUrl, String message) {
//        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
//                .
//                .build();
//        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        try {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build());
            System.out.println("\nmessage sent:\n" + message);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(1); //TODO: exception?
        }
    }

    public static String uploadFileToS3(File localFile, String key_name){
        // Get s3
        String bucket_name = "oo-dspsp-ass1";
//        String bucket_name = "dsps-221";

        // Upload input to S3
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket_name)
                .key(key_name)
//                .acl(ObjectCannedACL.PUBLIC_READ) //Access control list
                .build(), RequestBody.fromFile(localFile));
        System.out.println("\nfile uploaded to:\n" + "s3://"+bucket_name+"/"+key_name);
        return "s3://"+bucket_name+"/"+key_name;
    }

    public static String generate_keyName(String path){//TODO: check if needed.
        String[] splitted = path.split("/");
        String newKey = splitted[splitted.length-1].split(".pdf")[0] + "_" + new Date().getTime();
        return  newKey;
    }
}
