import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.fit.pdfdom.PDFDomTree;

import javax.xml.parsers.ParserConfigurationException;

public class Test {
    public static void  main(String[] args) throws IOException, ParserConfigurationException {
        URL url = new URL("https://faculty.washington.edu/stuve/log_error.pdf");
       // File file = new File(new URL(path).getFile());
        InputStream is = url.openStream();
        //byte[] array = FileUtils.readFileToByteArray(file);

//        PDDocument document = PDDocument.load(is);
//        PDFRenderer pdfRenderer = new PDFRenderer(document);
//        int pageCounter = 0;
//        for (PDPage page : document.getPages())
//        {
//            // note that the page number parameter is zero based
//            BufferedImage bim = pdfRenderer.renderImageWithDPI(pageCounter, 300, ImageType.RGB);
//
//            // suffix in filename will be used as the file format
//            ImageIOUtil.writeImage(bim, "./test" + "-" + (pageCounter++) + ".png", 300);
//        }
//        document.close();

        PDDocument pdf = PDDocument.load(is);
        PDDocument page = new PDDocument();
        page.addPage(pdf.getPage(3));
        //PDDocument document = PDDocument.load(is);
//        PDFRenderer pdfRenderer = new PDFRenderer(document);
//        int pageCounter = 0;
//        for (PDPage page : document.getPages())
//        {
//            // note that the page number parameter is zero based
//            BufferedImage bim = pdfRenderer.renderImageWithDPI(pageCounter, 300, ImageType.RGB);
//
//            // suffix in filename will be used as the file format
//            ImageIOUtil.writeImage(bim, "./test" + "-" + (pageCounter++) + ".png", 300);
//        }
//        document.close();
        Writer output = new PrintWriter("./pdf.html", "utf-8");
        new PDFDomTree().writeText(page, output);
        output.close();

    }
}
