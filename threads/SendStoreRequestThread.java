package threads;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class SendStoreRequestThread implements Runnable{
    DataOutputStream dos;
    BufferedReader in;
    Integer host_id;
    ConcurrentHashMap<String,String> total_map;
    public SendStoreRequestThread(DataOutputStream dos,BufferedReader in,Integer host_id,ConcurrentHashMap<String,String> total_map){
        this.dos = dos;
        this.in = in;
        this.host_id = host_id;
        this.total_map = total_map;
    }
    public void run(){
        String request = "STORE" + "\n";
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
            System.out.println("STORE RESP:"+response);
            ConcurrentHashMap<String,String> recvd_hm = new ConcurrentHashMap<String,String>();
            String[] rows = response.split("\\|");
            System.out.println("ROWS:"+Arrays.toString(rows));
            for(int i=0;i<rows.length;i++){
                String[] kv = rows[i].split("\\ ");
                recvd_hm.put(kv[0], kv[1]);
            }
            total_map.putAll(recvd_hm);
            dos.flush();
        } catch(Exception e){e.printStackTrace();}
    }
}
