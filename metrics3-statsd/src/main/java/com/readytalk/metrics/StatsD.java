package com.readytalk.metrics;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client to a StatsD server.
 */
@NotThreadSafe
public class StatsD implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(StatsD.class);

	private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private final InetSocketAddress address;
	private final DatagramSocketFactory socketFactory;

	private DatagramSocket socket;
	private int failures;

	/**
	 * Creates a new client which connects to the given address using the default
	 * {@link DatagramSocketFactory}.
	 *
	 * @param address the address of the StatsD server
	 */
	public StatsD(final InetSocketAddress address) {
		this(address, new DatagramSocketFactory());
	}

	/**
	 * Creates a new client which connects to the given address and socket factory.
	 *
	 * @param address the address of the Carbon server
	 * @param socketFactory the socket factory
	 */
	public StatsD(final InetSocketAddress address, final DatagramSocketFactory socketFactory) {
		this.address = address;
		this.socketFactory = socketFactory;
	}

	/**
	 * Connects to the server.
	 *
	 * @throws IllegalStateException if the client is already connected
	 * @throws IOException           if there is an error connecting
	 */
	public void connect() throws IOException {
		if (socket != null) {
			throw new IllegalStateException("Already connected");
		}

		this.socket = socketFactory.createSocket();
	}

	/**
	 * Sends the given measurement to the server. Logs exceptions.
	 *
	 * @param name the name of the metric
	 * @param value the value of the metric
	 */
	public void send(final String name, final String value) {
		try {
			String formatted = String.format("%s:%s|g", sanitize(name), sanitize(value));
			byte[] bytes = formatted.getBytes(UTF_8);
			socket.send(socketFactory.createPacket(bytes, bytes.length, address));
			failures = 0;
		} catch (IOException e) {
			failures++;

			if (failures == 1) {
				LOG.warn("unable to send packet to statsd at '{}:{}'", address.getHostName(), address.getPort());
			} else {
				LOG.debug("unable to send packet to statsd at '{}:{}'", address.getHostName(), address.getPort());
			}
		}
	}

	/**
	 * Returns the number of failed writes to the server.
	 *
	 * @return the number of failed writes to the server
	 */
	public int getFailures() {
		return failures;
	}

	@Override
	public void close() throws IOException {
		if (socket != null) {
			socket.close();
		}
		this.socket = null;
	}

	private String sanitize(final String s) {
		return WHITESPACE.matcher(s).replaceAll("-");
	}
}
