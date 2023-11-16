package threads;

import java.net.Socket;
import java.util.Arrays;

import storage.KeyValueStore;

public class RequestHandlerThread implements Runnable{
    Socket socket;
    KeyValueStore kv_store;
    public RequestHandlerThread(Socket sock,KeyValueStore kv_store){
        this.socket = sock;
        this.kv_store = kv_store;
    }
    public void run(){
        while(true){
            try{
                char buf = '\0';
                String query = "";
                while(!(buf == '\n')){
                    buf = (char) socket.getInputStream().read();
                    query += buf;
                }
                String[] query_terms = query.trim().split("\\ ");
                System.out.println("QUERY RECEIVED:"+Arrays.toString(query_terms));
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
                } else if(query_terms[0].equals("PTUPDATE")){
                    String key = query_terms[1];
                    int host_id = Integer.parseInt(query_terms[2]);
                    kv_store.handle_ptupdate(key, host_id, socket);
                } else if(query_terms[0].equals("EXIT")){
                    int host_id = Integer.parseInt(query_terms[1]);
                    kv_store.handle_exit(host_id,socket);
                }
                else{
                    System.out.println("INVALID QUERY RECEIVED");
                }
                System.out.println("**RECEIVED QUERY PROCESSING FINISHED**");
            } catch(Exception e){System.out.println("Exception in RequestHandlerThread");}
        }
    }
}
