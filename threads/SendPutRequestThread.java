package threads;
import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicInteger;

import service.StorageService;

public class SendPutRequestThread implements Runnable{
    String key;
    String value;
    StorageService host_stub;
    int host_id;
    AtomicInteger count;
    int request_random;
    public SendPutRequestThread(String key,String value,int host_id,StorageService host_stub,AtomicInteger count,Integer request_random){
        this.key = key;
        this.value = value;
        this.host_stub = host_stub;
        this.host_id = host_id;
        this.count = count;
        this.request_random = request_random;
    }
    public void run(){
        try{
            String response = host_stub.put(key,value,request_random);
            if(response.equals("YES")){
                count.incrementAndGet();
            }
        } catch(RemoteException e){
            System.out.println("Broadcast of put request failed to host:"+host_id);
        }
    }
}
