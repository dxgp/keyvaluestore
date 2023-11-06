package threads;
import java.io.BufferedReader;
import java.io.DataOutputStream;


public class SendGetRequestThread extends BaseSendRequestThread {
    

    public SendGetRequestThread(DataOutputStream dos, BufferedReader in, String req){
        super(dos, in, req);
    }

    public void run() {
        this.sendRequest();
        String valueForKey = this.recieveResponse();
        System.out.println("VALUE FOR KEY IS" + ": " + valueForKey);
    }
}
