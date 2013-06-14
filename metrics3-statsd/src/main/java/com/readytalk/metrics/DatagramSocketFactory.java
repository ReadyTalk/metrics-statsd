package com.readytalk.metrics;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

public class DatagramSocketFactory {
  public DatagramSocket createSocket() throws SocketException {
    return new DatagramSocket();
  }

  public DatagramPacket createPacket(byte[] bytes, int length, InetSocketAddress address) throws SocketException {
    return new DatagramPacket(bytes, length, address);
  }
}
