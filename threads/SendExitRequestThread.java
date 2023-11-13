package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class SendExitRequestThread implements Runnable{
    Integer peer_host_id, self_host_id;
    InetAddress address;
    DatagramSocket socket;
    int port;
    
    public SendExitRequestThread(Integer peer_host_id, String address, Integer self_host_id) throws UnknownHostException, SocketException{
        this.peer_host_id = peer_host_id;
        this.self_host_id = self_host_id;
        this.address = InetAddress.getByName(address);
        this.port = 10000 + peer_host_id;
        this.socket = new DatagramSocket();
    }

    public String sendRequest(String req) {
        try {
            // Send EXIT request
            byte[] buf = req.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, this.address, this.port);
            this.socket.send(packet);

            System.out.println("REQ Sent: " + req + " To host_id: " + this.peer_host_id);
            
            // Receive ack from peer
            byte[] ack_buf = new byte[100];
            DatagramPacket ack_packet = new DatagramPacket(ack_buf, ack_buf.length);
            socket.receive(ack_packet);
            String received = new String(ack_packet.getData(), 0, ack_packet.getLength());
            System.out.println("Recvd: " + received + " from host " + this.peer_host_id);
            return received;
        } catch (IOException e) {
            // TODO: handle exception
            System.out.println(e);
            return "";
        }
        
    }

    public void run(){
        String request = "EXIT " + this.self_host_id;
        try{
            String response = this.sendRequest(request);
            if(response.equals("OK")){
                System.out.println("RECEIVED OK FROM HOST " + this.peer_host_id);
            } else {
                System.out.println("FAILED TO EXECUTE EXIT AT HOST " + this.peer_host_id);
            }

        } catch(Exception e){}
    }
}
