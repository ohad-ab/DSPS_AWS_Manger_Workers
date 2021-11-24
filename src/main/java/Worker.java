import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
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
    private static final SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();


    public static void main(String[] args) throws IOException, ParserConfigurationException {
//        Just an idea, we don't have to do it this way
        String sqsUrl = args[0];
        String[] receivedMessage = waitForMessage(sqsUrl).split("\t");
        String operation = receivedMessage[0];
        URL url = new URL(receivedMessage[1]); //TODO: catch exeptions
        File localFile = null;
        String outputMessage = null;
        localFile = handleOperation(operation);
        if (localFile != null) {
            outputMessage = uploadFileToS3(localFile) + "\t" + operation;
            sendMessage(sqsUrl, outputMessage);
        }
    }

    public static File handleOperation(String operation) {

        switch (operation) { //TODO: catch exeptions
            case "ToImage":
                return new File(toImage(url));
            break;
            case "ToHTML":
                return new File(toHTML(url));
            break;
            case "ToText":
                return new File(toText(url));
            break;
            default:
                System.err.println("Operation not defined"); //TODO: throw exception?
        }
        return null;
    }


    public static String waitForMessage(String sqsurl) {
        Boolean gotMessage = false;
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        String body = null;
        while (!gotMessage) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(sqsurl).build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                body = message.body();
                gotMessage = true;
            }
        }
        return body;

    }

//    TODO: check if it's ok to return the local path (maybe we need ebs?)

    //    TODO: can we assume that it's only one image?
    public static String toImage(URL url) throws IOException {
        InputStream is = url.openStream();

        PDDocument document = PDDocument.load(is);
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        int pageCounter = 0;
        for (PDPage page : document.getPages()) {
            // note that the page number parameter is zero based
            BufferedImage bim = pdfRenderer.renderImageWithDPI(pageCounter, 300, ImageType.RGB);

            // suffix in filename will be used as the file format
            ImageIOUtil.writeImage(bim, "./output/Images/pdfImage" + "-" + (++pageCounter) + ".png", 300);
        }
        document.close();
        is.close();
        return "./output/Images/";
    }

    public static String toHTML(URL url) throws IOException, ParserConfigurationException {
        InputStream is = url.openStream();
        PDDocument pdf = PDDocument.load(is);
        PDFText2HTML stripper = new PDFText2HTML();
        PDDocument page = new PDDocument();
        page.addPage(pdf.getPage(0));
        Writer output = new PrintWriter("./output/HTML/pdf.html", "utf-8");
        new PDFDomTree().writeText(page, output);
        output.close();
        page.close();
        pdf.close();
        is.close();
        return "./output/HTML/pdf.html";


    }

    public static String toText(URL url) throws IOException {
        InputStream is = url.openStream();
        PDDocument document = PDDocument.load(is);
        PDFTextStripper textStripper = new PDFTextStripper();
        String str = textStripper.getText(document);
        PrintWriter outTxt = new PrintWriter("./output/Texts/pdf.txt");
        outTxt.println(str);
        outTxt.close();
        document.close();
        is.close();
        return "./output/Texts/pdf.txt";
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

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(1); //TODO: exception?
        }
    }

    public static String uploadFileToS3(File localFile){
        // Get s3
//        String bucket_name = "oo-dspsp-ass1";
        String bucket_name = "dsps-221";

        // Upload input to S3
        String key_name = generate_keyName();
        s3.putObject(PutObjectRequest.builder().bucket(bucket_name).key(key_name).build(), RequestBody.fromFile(localFile));
        return "s3://"+bucket_name+"/"+key_name;
    }

    public static String generate_keyName(){//TODO: check if needed.
        String newKey = "inputTest";
        //TODO: Get available name
        return  newKey;
    }
}
