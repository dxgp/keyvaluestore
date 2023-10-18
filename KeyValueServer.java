import java.net.*;
import java.io.*;
import java.util.*;

public class KeyValueServer {
    private Map<String, String> data;
    KeyValueServer(){
        this.data = new HashMap<>();
    }
    public static void main(String[] args) throws IOException{
        /*the main thread will just accept incoming connections, 
          all queries from a certain client will be handled by a separate thread*/
        KeyValueServer kv_server = new KeyValueServer();
        ServerSocket inSocket = new ServerSocket(9999);
        inSocket.setReuseAddress(true);
        while(true){
            Socket connectionSocket = inSocket.accept();
            RequestHandlerThread client_handler = new RequestHandlerThread(connectionSocket, kv_server);
            new Thread(client_handler).start();
        }
    }
    /*
     * This is a class to create a thread that will infinitely loop and will accept queries from clients.
     */
    private static class RequestHandlerThread implements Runnable{
        private final KeyValueServer kv_server;
        private final Socket clientSocket;
        public RequestHandlerThread(Socket clientSocket,KeyValueServer kv_server){
            this.clientSocket = clientSocket;
            this.kv_server = kv_server;
        }
        public void run(){
            try{
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
