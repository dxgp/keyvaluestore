package threads;

import java.io.DataOutputStream;
import java.io.BufferedReader;

public class SendPTUpdateRequestThread implements Runnable {
    DataOutputStream dos;
    BufferedReader in;
    String key;
    Integer host_id;
    public SendPTUpdateRequestThread(DataOutputStream dos,BufferedReader in,String key,Integer host_id){
        this.dos = dos;
        this.in = in;
        this.key = key;
        this.host_id = host_id;
    }
    public void run(){
        String request = "PTUPDATE "+key+" "+host_id+"\n";
        try{
            dos.writeBytes(request);
            while(!in.ready());
            char buf = '\0';
            String response = "";
            while(!(buf == '\n')){
                buf = (char) in.read();
                response += buf;
            }
            response = response.trim();
            if(!response.equals("EXECUTED")){
                System.out.println("FAILED TO EXECUTE PTUPDATE for host "+host_id);
            }
            dos.flush();
        } catch(Exception e){e.printStackTrace();}
    }
}
