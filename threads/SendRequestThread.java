package threads;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class SendRequestThread implements Runnable {
    DataOutputStream dos;
    BufferedReader in;
    String req;
    AtomicInteger counter;
    TotalStore local_store;
    Boolean is_store = false;
    public SendRequestThread(DataOutputStream dos, BufferedReader in, String req, AtomicInteger counter){
        this.dos = dos;
        this.in = in;
        this.req = req;
        this.counter = counter;
    }
    public SendRequestThread(DataOutputStream dos, BufferedReader in, String req,TotalStore local_store) {
        this.dos = dos;
        this.in = in;
        this.req = req;
        this.local_store = local_store;
        this.is_store = true;
    }
    public SendRequestThread(DataOutputStream dos,BufferedReader in,String req){
        this.dos = dos;
        this.in = in;
        this.req = req;
    }
    public void run() {
        try {
            System.out.println("REQUEST SENT:" + req);
            dos.writeBytes(req);
            while(!in.ready());
            String data_recvd = in.readLine();
            if(!is_store){
                if(data_recvd.equals("YES")){
                    counter.incrementAndGet();
                }
            } else{
                System.out.println("ADDING TO LS");
                local_store.total += data_recvd;
            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
