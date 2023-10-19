import java.net.*;
import java.io.*;
import java.util.*;

public class KeyValueServer {
    private Map<String, String> data;
    static KeyValueServer kv_server;
    KeyValueServer(){
        this.data = new HashMap<>();
    }
    public static void main(String[] args) throws IOException{
        /*the main thread will just accept incoming connections, 
          all queries from a certain client will be handled by a separate thread*/
        ServerSocket inSocket = new ServerSocket(9999);
        inSocket.setReuseAddress(true);
        KeyValueServer.kv_server = new KeyValueServer();
        while(true){
            Socket connectionSocket = inSocket.accept();
            ClientHandlerThread client_handler = new ClientHandlerThread(connectionSocket);
            new Thread(client_handler).start();
        }
    }
    /*
     * This is a class to create a thread that will infinitely loop and will accept queries from clients.
     */
    private static class ClientHandlerThread implements Runnable{
        private final Socket clientSocket;
        public ClientHandlerThread(Socket clientSocket){
            this.clientSocket = clientSocket;
        }
        public void run(){
            try{
                /*
                 * Describes how the query from a certain client is handled.
                 * Inside the infinite loop, the query is processed.
                 */
                while(true){
                    BufferedReader inFromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    DataOutputStream outToClient = new DataOutputStream(new DataOutputStream(clientSocket.getOutputStream()));
                    while(!inFromClient.ready());
                    String clientQuery = inFromClient.readLine();
                    System.out.println(clientQuery);
                    String[] queryTerms = clientQuery.split("\\|");
                    System.out.println(Arrays.toString(queryTerms));
                    String result = kv_server.processQuery(queryTerms);
                    outToClient.writeBytes(result);
                    outToClient.flush();
                }
            } catch(IOException e){
                System.out.println(e.toString());
            }    
        }
    }
    private static class PutQueryHandler implements Runnable{
        String key;
        String value;
        PutQueryHandler(String key, String value){
            this.key = key;
            this.value = value;
        }
        public void run(){
            KeyValueServer.kv_server.data.put(key, value);
        }
    }
    private static class DeleteQueryHandler implements Runnable{
        String key;
        DeleteQueryHandler(String key){
            this.key = key;
        }
        public void run(){
            KeyValueServer.kv_server.data.remove(key)
        }
    }
    private static class GetQueryHandler implements Runnable{
        String key;
        GetQueryHandler(String key){
            this.key = key;
        }
        public void run(){
            KeyValueServer.kv_server.data.get(key);
        }
    }
    /*
     * A function to process the queries. Need to add code for the store function.
     * Maybe consider serialization?
     */
    public String processQuery(String[] queryTerms){
        if(queryTerms[0].equals("GET")){
            System.out.println("GET QUERY RECEIVED");
            return data.get(queryTerms[1])+"\n";
        }
        else if(queryTerms[0].equals("PUT")){
            System.out.println("PUT QUERY RECEIVED");
            data.put(queryTerms[1],queryTerms[2]);
            return "AB\n";
        }
        else if(queryTerms[0].equals("DEL")){
            System.out.println("DELETE QUERY RECEIVED");
            data.remove(queryTerms[1]);
            return "AB\n";
        }
        else if(queryTerms[0].equals("STORE")){
            System.out.println("STORE QUERY RECEIVED");
            return "AB\n";
        }
        else{
            System.out.println("Invalid query provided");
            return "AB";
        }
    }
}
