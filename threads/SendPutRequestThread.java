package threads;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.util.concurrent.atomic.AtomicInteger;


public class SendPutRequestThread extends BaseSendRequestThread {
    AtomicInteger counter;

    public SendPutRequestThread(DataOutputStream dos, BufferedReader in, String req, AtomicInteger counter){
        super(dos, in, req);
        this.counter = counter;
    }

    public void run() {
        this.sendRequest();
        String res = this.recieveResponse();
        if(res.equals("YES")){
            counter.incrementAndGet();
        }
    }
}
