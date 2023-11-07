package threads;

import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;

import service.StorageService;

public class SendStoreRequestThread implements Runnable{
    ConcurrentHashMap<String,String> combined_map;
    StorageService host_stub;
    public SendStoreRequestThread(StorageService host_stub,ConcurrentHashMap<String,String> combined_map){
        this.combined_map = combined_map;
        this.host_stub = host_stub;
    }
    public void run(){
        try{
            ConcurrentHashMap<String,String> response = host_stub.store();
            response.forEach((key,value)->{
                combined_map.putAll(response);
            });
        } catch(RemoteException e){
            System.out.println("RemoteException occured in SendStoreRequestThread");
        }
    }
}