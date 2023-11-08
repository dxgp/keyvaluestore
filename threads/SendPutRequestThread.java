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
            String response = "";
            dos.writeBytes(request);
            dos.flush();
            while(!in.ready());
            char buf = '\0';
            while(!(buf == '\n')){
                buf = (char) in.read();
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
