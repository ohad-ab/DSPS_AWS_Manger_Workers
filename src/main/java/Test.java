import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.xml.parsers.ParserConfigurationException;

public class Test {

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
        PDDocument page = new PDDocument();
        page.addPage(pdf.getPage(3));
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

        public static void  main(String[] args) throws IOException, ParserConfigurationException {
        File input = new File("input/input-sample-1.txt");
//        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
//        String bucket_name = "oo-dspsp-ass1";
//        String key_name = "inputTest";
//        ResponseInputStream<GetObjectResponse> test= s3.getObject(GetObjectRequest.builder().bucket(bucket_name).key(key_name).build());
//        BufferedReader reader = new BufferedReader(new InputStreamReader(test));
//
//        String line;
//        while ((line = reader.readLine()) != null) {
//            System.out.println(line);
//        }
//
//        s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket_name).key(key_name).build());
//        try {
//            S3Object o = s3.getObject(bucket_name, key_name);
//            S3ObjectInputStream s3is = o.getObjectContent();
//            FileOutputStream fos = new FileOutputStream(new File(key_name));
//            byte[] read_buf = new byte[1024];
//            int read_len = 0;
//            while ((read_len = s3is.read(read_buf)) > 0) {
//                fos.write(read_buf, 0, read_len);
//            }
//            s3is.close();
//            fos.close();
//        } catch (AmazonServiceException e) {
//            System.err.println(e.getErrorMessage());
//            System.exit(1);
//        } catch (FileNotFoundException e) {
//            System.err.println(e.getMessage());
//            System.exit(1);
//        } catch (IOException e) {
//            System.err.println(e.getMessage());
//            System.exit(1);

//        s3.putObject(PutObjectRequest.builder().bucket("oo-dspsp-ass1").key("inputTest").build(), RequestBody.fromFile(input));
        URL url = new URL("https://faculty.washington.edu/stuve/log_error.pdf");
        toHTML(url);
        toImage(url);
        toText(url);

    }
}
