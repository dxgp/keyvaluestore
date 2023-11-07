package threads;

import java.io.BufferedReader;
import java.io.DataOutputStream;

public class SendDeleteRequestThread implements Runnable{
    DataOutputStream dos;
    BufferedReader in;
    String key;
    public SendDeleteRequestThread(DataOutputStream dos,BufferedReader in,String key){
        this.dos = dos;
        this.in = in;
        this.key = key;
    }
    public void run(){
        String request = "DELETE "+key+"\n";
        try{
            dos.writeBytes(request);
            while(!in.ready());
            char buf = '\0';
            String response = "";
            while(!(buf == '\n')){
                buf = (char) in.read();
                response += buf;
            }
            if(!response.equals("EXECUTED")){
                System.out.println("FAILED TO EXECUTE DELETE");
            }
        } catch(Exception e){}
    }
}
