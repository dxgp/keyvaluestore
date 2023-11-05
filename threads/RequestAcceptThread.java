package threads;

import java.net.*;
import java.io.*;
import storage.KeyValueStore;

public class RequestAcceptThread implements Runnable{
    ServerSocket accept_socket;
    KeyValueStore kv_store;
    public RequestAcceptThread(int host_id,KeyValueStore kv_store){
        try{
            accept_socket = new ServerSocket(10000+host_id);
            this.kv_store = kv_store;
        } catch(IOException e){
            System.out.println(e.toString());
        }
    }
    public void run(){
        while(true){
            try{
                Socket socket = accept_socket.accept();
                // call the request handler thread here
                RequestHandlerThread req_handler = new RequestHandlerThread(socket,kv_store);
                new Thread(req_handler).start();
            } catch(IOException e){
                System.out.println(e.toString());
            }
        }
    }
}