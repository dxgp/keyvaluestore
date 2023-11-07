package threads;

import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.ConcurrentHashMap;

public class SendStoreRequestThread implements Runnable{
    DataOutputStream dos;
    ObjectInputStream ois;
    Integer host_id;
    ConcurrentHashMap<String,String> total_map;
    public SendStoreRequestThread(DataOutputStream dos,ObjectInputStream ois,Integer host_id,ConcurrentHashMap<String,String> total_map){
        this.dos = dos;
        this.ois = ois;
        this.host_id = host_id;
        this.total_map = total_map;
    }
    public void run(){
        String request = "STORE" + "\n";
        try{
            dos.writeBytes(request);
            ConcurrentHashMap<String,String> recvd = (ConcurrentHashMap<String,String>)ois.readObject();
            total_map.putAll(recvd);
        } catch(Exception e){}
    }
}
