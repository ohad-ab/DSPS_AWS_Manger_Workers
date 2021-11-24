import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.xml.parsers.ParserConfigurationException;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class Worker {
    public static void main(String[] args) throws IOException, ParserConfigurationException {
//        Just an idea, we don't have to do it this way
        String sqsUrl = args[0];
        String[] receivedMessage = waitForMessage(sqsUrl).split("\t");
        String operation = receivedMessage[0];
        URL url = new URL(receivedMessage[1]); //TODO: catch exeptions
        switch (operation){ //TODO: catch exeptions
            case "ToImage":
                toImage(url);
                break;
            case "ToHTML":
                toHTML(url);
                break;
            case "ToText":
                toText(url);
                break;
            default:
                System.out.println("Operation not defined"); //TODO: throw exception?
        }








    }

//    TODO: implement this
    public static String waitForMessage(String sqsurl){
        Boolean gotMessage = false;
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        String body = null;
        while (!gotMessage){
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(sqsurl).build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            Message message = messages.get(0);
            body= message.body();
            if(body.equals("terminate"))
            {
                System.out.println("ee");
                gotMessage = true;
                Ec2Client ec2 = Ec2Client.create();
                List<Reservation> reservations = ec2.describeInstances().reservations();
                for (Reservation reservation:reservations) {
                    Instance instance = reservation.instances().get(0);
                    if(!instance.tags().isEmpty() && instance.tags().get(0).value().equals("Manager") && (instance.state().nameAsString().equals("running")))
                        ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
                }
            }
        }
        return body;

    }
//    public static void sqsTry() {
//        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
//
//
//        try {
//            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
//            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
//
//            String queueUrl = listQueuesResponse.queueUrls().get(0);
////            Send message
////            SendMessageRequest send_msg_request = SendMessageRequest.builder()
////                    .queueUrl(queueUrl)
////                    .messageBody("s3://dsps-221/pdf.html")
////                    .build();
////            sqs.sendMessage(send_msg_request);
////
//            while (!terminate)
//            {
//
//                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
//                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
//                Message message = messages.get(0);
//                String body= message.body();
//                if(body.equals("terminate"))
//                {
//                    System.out.println("ee");
//                    terminate = true;
//                    Ec2Client ec2 = Ec2Client.create();
//                    List<Reservation> reservations = ec2.describeInstances().reservations();
//                    for (Reservation reservation:reservations) {
//                        Instance instance = reservation.instances().get(0);
//                        if(!instance.tags().isEmpty() && instance.tags().get(0).value().equals("Manager") && (instance.state().nameAsString().equals("running")))
//                            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
//                    }
//                }
//
//
//
//            }
//
//        }
//        catch (SqsException e) {
//            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(1);
//        }
//    }
    public static void toImage(URL url) throws IOException {
        InputStream is = url.openStream();

        PDDocument document = PDDocument.load(is);
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        int pageCounter = 0;
        for (PDPage page : document.getPages())
        {
            // note that the page number parameter is zero based
            BufferedImage bim = pdfRenderer.renderImageWithDPI(pageCounter, 300, ImageType.RGB);

            // suffix in filename will be used as the file format
            ImageIOUtil.writeImage(bim, "./output/Images/pdfImage" + "-" + (++pageCounter) + ".png", 300);
        }
        document.close();
        is.close();
    }

    public static void toHTML(URL url) throws IOException, ParserConfigurationException {
        InputStream is = url.openStream();
        PDDocument pdf = PDDocument.load(is);
        PDFText2HTML stripper =new PDFText2HTML();
        PDDocument page = new PDDocument();
        page.addPage(pdf.getPage(0));
        Writer output = new PrintWriter("./output/HTML/pdf.html", "utf-8");
        new PDFDomTree().writeText(page, output);
        output.close();
        page.close();
        pdf.close();
        is.close();


    }

    public static void toText(URL url) throws IOException {
        InputStream is = url.openStream();
        PDDocument document = PDDocument.load(is);
        PDFTextStripper textStripper = new PDFTextStripper();
        String str = textStripper.getText(document);
        PrintWriter outTxt = new PrintWriter("./output/Texts/pdf.txt");
        outTxt.println(str);
        outTxt.close();
        document.close();
        is.close();
    }
}
