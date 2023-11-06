package threads;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;


public class BaseSendRequestThread implements Runnable{
    DataOutputStream dos;
    String req;
    BufferedReader in;

    public BaseSendRequestThread(DataOutputStream dos, BufferedReader in, String req){
        this.dos = dos;
        this.req = req;
        this.in = in;
    }

    public void sendRequest() {
        try {
            this.dos.writeBytes(this.req);
            System.out.println("REQUEST SENT:" + req);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public String recieveResponse() {
        try {
            while(!this.in.ready());
            char[] buf = new char[100];
            in.read(buf);
            String res = new String(buf);
            return res;
        } catch (IOException e) {
            System.err.println(e);
            return e.getMessage();
        }
    }

    public void run() {
        this.sendRequest();
    }
}
