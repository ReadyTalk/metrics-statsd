package com.readytalk.metrics;

import static org.junit.Assert.assertNotNull;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

import org.junit.Test;

public class DatagramSocketFactoryTest {
	@Test
	public void createDatagramSocket() throws SocketException {
		DatagramSocket socket = new DatagramSocketFactory().createSocket();
		assertNotNull(socket);
	}

	@Test
	public void createDatagramPacket() throws SocketException {
		InetSocketAddress address = new InetSocketAddress("example.com", 9876);
		DatagramPacket packet = new DatagramSocketFactory().createPacket("yellow dellow".getBytes(),
				"yellow dellow".length(), address);
		assertNotNull(packet);
	}
}
