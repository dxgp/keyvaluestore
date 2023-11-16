package threads;

import java.net.Socket;

/*
 * A thread for sending the exit request.
 */
public class SendExitRequestThread implements Runnable{
    Socket sock;
    Integer host_id;
    public SendExitRequestThread(Socket sock,Integer host_id){
        this.sock = sock;
        this.host_id = host_id;
    }
    public void run(){
        String request = "EXIT "+host_id + "\n";
        try{
            String response = "";
            sock.getOutputStream().write(request.getBytes());
            sock.getOutputStream().flush();
            char buf = '\0';
            while(!(buf == '\n')){
                buf = (char) sock.getInputStream().read();
                response += buf;
            }
            response = response.trim();
        } catch(Exception e){}
    }
}
