package threads;

import service.StorageService;

public class SendPTUpdateThread implements Runnable {
    String key;
    int host_id;
    StorageService host_stub;
    public SendPTUpdateThread(String key,int host_id,StorageService host_stub){
        this.key = key;
        this.host_id = host_id;
        this.host_stub = host_stub;
    }
    public void run(){
        try{
            host_stub.ptupdate(key,host_id);
        } catch(Exception e){
            System.out.println("An exception occured in SendPTUpdateThread");
        }
    }
}
