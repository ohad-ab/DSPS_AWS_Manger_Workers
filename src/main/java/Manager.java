import java.util.concurrent.LinkedBlockingQueue;
public class Manager implements Runnable {

    private LinkedBlockingQueue<InFile> queue;
    @Override
    public void run() {
//        LinkedBlockingQueue<InFile> queue = new LinkedBlockingQueue<>();
//        String line;
//        while ((line = br.readLine()) != null) {
//            String[] splitted = line.split("\t");
//            InFile f = new InFile(splitted[0], splitted[1]);
//            queue.add(f);
//        }
    }
}
