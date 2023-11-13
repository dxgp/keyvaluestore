package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class SendDeleteRequestThread implements Runnable{
    String key;
    Integer host_id;
    InetAddress address;
    DatagramSocket socket;
    int port;
    
    public SendDeleteRequestThread(Integer host_id, String address, String key) throws UnknownHostException, SocketException{
        this.key = key;
        this.host_id = host_id;
        this.address = InetAddress.getByName(address);
        this.port = 10000 + host_id;
        this.socket = new DatagramSocket();
    }

    public String sendRequest(String req) {
        try {
            // Send DELETE request
            byte[] buf = req.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, this.address, this.port);
            this.socket.send(packet);

            System.out.println("REQ Sent: " + req + " To host_id: " + this.host_id);
            
            // Receive ack from peers
            byte[] ack_buf = new byte[100];
            DatagramPacket ack_packet = new DatagramPacket(ack_buf, ack_buf.length);
            socket.receive(ack_packet);
            String received = new String(ack_packet.getData(), 0, ack_packet.getLength());
            System.out.println("Recvd: " + received + " from host " + this.host_id);
            return received;
        } catch (IOException e) {
            // TODO: handle exception
            System.out.println(e);
            return "";
        }
        
    }

    public void run(){
        String request = "DELETE " + key;
        try{
            String response = this.sendRequest(request);
            if(!response.equals("EXECUTED")){
                System.out.println("HOST " + this.host_id + " FAILED TO EXECUTE DELETE");
            } else {
                System.out.println("HOST " + this.host_id + " HAS EXECUTED DELETE");
            }

        } catch(Exception e){}
    }
}
