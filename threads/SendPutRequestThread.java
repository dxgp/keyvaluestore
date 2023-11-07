package threads;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class SendPutRequestThread implements Runnable {
    DataOutputStream dos;
    BufferedReader in;
    String key;
    String value;
    AtomicInteger count;
    Integer self_random;
    public SendPutRequestThread(DataOutputStream dos,BufferedReader in,String key,String value,AtomicInteger count,Integer self_random){
        this.dos = dos;
        this.in = in;
        this.key = key;
        this.value = value;
        this.count = count;
        this.self_random = self_random;
    }
    public void run(){
        String request = "PUT "+key+" "+value+" "+self_random+"\n";
        try{
            dos.writeBytes(request);
            char buf = '\0';
            String response = "";
            while(!in.ready());
            while(!(buf == '\n')){
                buf = (char) in.read();
                response += buf;
            }
            response = response.trim();
            if(response.equals("YES")){
                count.incrementAndGet();
            }
            dos.flush();
        } catch(Exception e){
            System.out.println("Exception occured when writing query to output stream in SendPutRequestThread");
        }
    }
    
}
