package threads;

import java.rmi.RemoteException;

import service.StorageService;

public class DeleteRequestThread implements Runnable{
    StorageService host_stub;
    String key;
    public DeleteRequestThread(StorageService host_stub,String key){
        this.host_stub = host_stub;
        this.key = key;
    }
    public void run(){
        try{
            host_stub.delete(key);
        } catch(RemoteException e){
            System.out.println("Remote exception in DeleteRequestThread");
        }
    }
}
