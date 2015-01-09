package com.google.code.gossip.manager.impl;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.json.JSONArray;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.manager.ActiveGossipThread;
import com.google.code.gossip.manager.GossipManager;

abstract public class SendMembersActiveGossipThread extends ActiveGossipThread {

	public SendMembersActiveGossipThread(GossipManager gossipManager) {
		super(gossipManager);
	}
	
	/**
	 * Performs the sending of the membership list, after we have
	 * incremented our own heartbeat.
	 */
	protected void sendMembershipList(LocalGossipMember me, ArrayList<LocalGossipMember> memberList) {
		GossipService.debug("Send sendMembershipList() is called.");

		// Increase the heartbeat of myself by 1.
		me.setHeartbeat(me.getHeartbeat() + 1);
		
		synchronized (memberList) {
			try {
				LocalGossipMember member = selectPartner(memberList);

				if (member != null) {
					InetAddress dest = InetAddress.getByName(member.getHost());
					
					// Create a StringBuffer for the JSON message.
					JSONArray jsonArray = new JSONArray();
					GossipService.debug("Sending memberlist to " + dest + ":" + member.getPort());
					GossipService.debug("---------------------");
					
					// First write myself, append the JSON representation of the member to the buffer.
					jsonArray.put(me.toJSONObject());
					GossipService.debug(me);
					
					// Then write the others.
					for (int i=0; i<memberList.size(); i++) {
						LocalGossipMember other = memberList.get(i);
						// Append the JSON representation of the member to the buffer.
						jsonArray.put(other.toJSONObject());
						GossipService.debug(other);
					}
					GossipService.debug("---------------------");
					
					// Write the objects to a byte array.
					byte[] json_bytes = jsonArray.toString().getBytes();
					
					int packet_length = json_bytes.length;
					
					if (packet_length < GossipManager.MAX_PACKET_SIZE) {
						
						// Convert the packet length to the byte representation of the int.
						byte[] length_bytes = new byte[4];
						length_bytes[0] =(byte)(  packet_length >> 24 );
						length_bytes[1] =(byte)( (packet_length << 8) >> 24 );
						length_bytes[2] =(byte)( (packet_length << 16) >> 24 );
						length_bytes[3] =(byte)( (packet_length << 24) >> 24 );
						
						
						GossipService.debug("Sending message ("+packet_length+" bytes): " + jsonArray.toString());
						
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + json_bytes.length);
						byteBuffer.put(length_bytes);
						byteBuffer.put(json_bytes);
						byte[] buf = byteBuffer.array();
						
						DatagramSocket socket = new DatagramSocket();
						DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, dest, member.getPort());
						socket.send(datagramPacket);
						socket.close();
					} else {
						GossipService.error("The length of the to be send message is too large (" + packet_length + " > " + GossipManager.MAX_PACKET_SIZE + ").");
					}
				}

			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}
