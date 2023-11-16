package threads;

import java.rmi.RemoteException;

import service.StorageService;

public class SendExitRequestThread implements Runnable{
    Integer host_id;
    StorageService host_stub;
    public SendExitRequestThread(StorageService host_stub,Integer host_id){
        this.host_id = host_id;
        this.host_stub = host_stub;
    }
    public void run(){
        try{
            host_stub.exit(host_id);
        } catch(RemoteException e){
            System.out.println("Remote exception in sending exit request");
        }
    }
}
