package threads;
import java.net.Socket;

/*
 * A thread for sending the delete request thread.
 */
public class SendDeleteRequestThread implements Runnable{
    Socket sock;
    String key;
    public SendDeleteRequestThread(Socket sock,String key){
        this.sock = sock;
        this.key = key;
    }
    public void run(){
        String request = "DELETE "+key+"\n";
        try{
            sock.getOutputStream().write(request.getBytes());
            sock.getOutputStream().flush();
            char buf = '\0';
            String response = "";
            while(!(buf == '\n')){
                buf = (char) sock.getInputStream().read();
                response += buf;
            }
            response = response.trim();
            if(!response.equals("EXECUTED")){
                System.out.println("FAILED TO EXECUTE DELETE");
            }
        } catch(Exception e){e.printStackTrace();}
    }
}
