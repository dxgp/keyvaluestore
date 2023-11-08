package threads;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.Socket;

public class SendPutRequestThread implements Runnable {
    Socket sock;
    String key;
    String value;
    AtomicInteger count;
    Integer self_random;
    public SendPutRequestThread(Socket sock,String key,String value,AtomicInteger count,Integer self_random){
        this.sock = sock;
        this.key = key;
        this.value = value;
        this.count = count;
        this.self_random = self_random;
    }
    public void run(){
        String request = "PUT "+key+" "+value+" "+self_random+"\n";
        try{
            String response = "";
            // dos.writeBytes(request);
            // dos.flush();
            sock.getOutputStream().write(request.getBytes());
            char buf = '\0';
            while(!(buf == '\n')){
                buf = (char) sock.getInputStream().read();
                response += buf;
            }
            response = response.trim();
            if(response.equals("YES")){
                count.incrementAndGet();
            }
        } catch(Exception e){
            System.out.println("Exception occured when writing query to output stream in SendPutRequestThread");
            e.printStackTrace();
        }
    }
    
}
