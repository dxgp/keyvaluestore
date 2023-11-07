package threads;

import storage.KeyValueStore;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class RequestListenThread implements Runnable{
    int host_id;
    KeyValueStore kv_store;
    ServerSocket accept_socket;
    Socket socket;
    public RequestListenThread(int host_id,KeyValueStore kv_store){
        try{
            this.host_id = host_id;
            this.kv_store = kv_store;
            accept_socket = new ServerSocket(10000+host_id);
        } catch(Exception e){}
    }
    public void run(){
        System.out.println("Now listening for incoming connections");
        try{
            socket = accept_socket.accept();
            BufferedReader sender_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while(true){
                while(!sender_in.ready());
                char buf = '\0';
                String query = "";
                while(!(buf == '\n')){
                    buf = (char) sender_in.read();
                    query += buf;
                }
                String[] query_terms = query.split("// ");
                if(query_terms[0].equals("PUT")){
                    String key = query_terms[1];
                    String value = query_terms[2];
                    int recvd_rand = Integer.parseInt(query_terms[3]);
                    kv_store.handle_put(key, value, recvd_rand, socket);
                } else if(query_terms[0].equals("GET")){
                    String key = query_terms[1];
                    kv_store.handle_get(key, socket);
                } else if(query_terms[0].equals("STORE")){
                    kv_store.handle_store(socket);
                } else if(query_terms[0].equals("DELETE")){
                    String key = query_terms[1];
                    kv_store.handle_delete(key, socket);
                } else{
                    System.out.println("INVALID QUERY RECEIVED");
                }
            }
        } catch(Exception e){}
    }
}
