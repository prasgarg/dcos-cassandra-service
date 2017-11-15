package com.mesosphere.dcos.cassandra.executor;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;

public class CloudUtils {
	private static final String AWS_PUBLIC_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/public-ipv4";
	private static final String AZURE_PUBLIC_IP_QUERY_URL = "http://169.254.169.254/metadata/instance/network/interface/0/ipv4/ipAddress/0/publicIpAddress?api-version=2017-04-02&format=text";

	public static String getPublicAwsPublicIp() throws UnknownHostException, IOException {
		try {
			String publicIp = awsApiCall(AWS_PUBLIC_IP_QUERY_URL);
			if (publicIp == null || "".equals(publicIp)) {
				return null;
			}
			InetAddress localPublicAddress = InetAddress.getByName(publicIp);
			return localPublicAddress.getHostAddress();
		} catch (ConfigurationException configurationException) {
			return null;
		}
	}

	public static String getPublicAzurePublicIp() throws UnknownHostException, IOException {
		try {
			String publicIp = azureApiCall(AZURE_PUBLIC_IP_QUERY_URL);
			if (publicIp == null || "".equals(publicIp)) {
				return null;
			}
			InetAddress localPublicAddress = InetAddress.getByName(publicIp);
			return localPublicAddress.getHostAddress();
		} catch (ConfigurationException configurationException) {
			return null;
		}
	}

	public static String awsApiCall(String url) throws IOException, ConfigurationException {
		// Populate the region and zone by introspection, fail if 404 on
		// metadata
		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		DataInputStream d = null;
		try {
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != 200)
				throw new ConfigurationException("Ec2Snitch was unable to execute the API call. Not an ec2 node?");

			int cl = conn.getContentLength();
			byte[] b = new byte[cl];
			d = new DataInputStream((FilterInputStream) conn.getContent());
			d.readFully(b);
			return new String(b, StandardCharsets.UTF_8);
		} finally {
			FileUtils.close(d);
			conn.disconnect();
		}
	}

	public static String azureApiCall(final String url) throws IOException, ConfigurationException {
		// Populate the region and zone by introspection, fail if 404 on
		// metadata
		final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		DataInputStream d = null;
		try {
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Metadata", "true");
			if (conn.getResponseCode() != 200)
				throw new ConfigurationException("MdsAzureSnitch was unable to execute the API call. Not an azure VM?");

			final int cl = conn.getContentLength();
			final byte[] b = new byte[cl];
			d = new DataInputStream((FilterInputStream) conn.getContent());
			d.readFully(b);
			return new String(b, StandardCharsets.UTF_8);
		} finally {
			FileUtils.close(d);
			conn.disconnect();
		}
	}
}
