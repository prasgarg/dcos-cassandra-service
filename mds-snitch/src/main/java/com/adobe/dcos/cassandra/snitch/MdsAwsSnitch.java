
package com.adobe.dcos.cassandra.snitch;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.ReconnectableSnitchHelper;
import org.apache.cassandra.locator.SnitchProperties;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A snitch that assumes an EC2 region is a DC and an EC2 availability_zone is a
 * rack. This information is available in the config for the node.
 */
public class MdsAwsSnitch extends AbstractNetworkTopologySnitch {

	private static final Logger LOGGER = LoggerFactory.getLogger(MdsAwsSnitch.class);

	protected static final String ZONE_NAME_QUERY_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
	private static final String PUBLIC_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/public-ipv4";
    private static final String PRIVATE_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/local-ipv4";
    
	private static final String DEFAULT_DC = "UNKNOWN-DC";
	private static final String DEFAULT_RACK = "UNKNOWN-RACK";
	private Map<InetAddress, Map<String, String>> savedEndpoints;
	protected String ec2zone;
	protected String ec2region;
	private final String CLOUD_PROVIDER_PREFIX = "AWS-"; 
	private final String localPrivateAddress;
	
	public MdsAwsSnitch() throws IOException, ConfigurationException {
		String az = awsApiCall(ZONE_NAME_QUERY_URL);
		// Split "us-east-1a"  into "us-east-1" , "a" .
		ec2zone = "" + az.charAt(az.length() - 1);
		ec2region = CLOUD_PROVIDER_PREFIX + az.substring(0, az.length() - 1);
		String datacenterSuffix = (new SnitchProperties()).get("dc_suffix", "");
		ec2region = ec2region.concat(datacenterSuffix);
		LOGGER.info("EC2Snitch using region: {}, zone: {}.", ec2region, ec2zone);
		
		InetAddress localPublicAddress = InetAddress.getByName(awsApiCall(PUBLIC_IP_QUERY_URL));
		LOGGER.info("MdsAwsSnitch using publicIP as identifier: {}", localPublicAddress);
        localPrivateAddress = awsApiCall(PRIVATE_IP_QUERY_URL);
        // use the Public IP to broadcast Address to other nodes.
        DatabaseDescriptor.setBroadcastAddress(localPublicAddress);
        if (DatabaseDescriptor.getBroadcastRpcAddress() == null)
        {
            LOGGER.info("broadcast_rpc_address unset, broadcasting public IP as rpc_address: {}", localPublicAddress);
            DatabaseDescriptor.setBroadcastRpcAddress(localPublicAddress);
        }
	}

	String awsApiCall(String url) throws IOException, ConfigurationException {
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

	public String getRack(InetAddress endpoint) {
		if (endpoint.equals(FBUtilities.getBroadcastAddress()))
			return ec2zone;
		EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
		if (state == null || state.getApplicationState(ApplicationState.RACK) == null) {
			if (savedEndpoints == null)
				savedEndpoints = SystemKeyspace.loadDcRackInfo();
			if (savedEndpoints.containsKey(endpoint))
				return savedEndpoints.get(endpoint).get("rack");
			return DEFAULT_RACK;
		}
		return state.getApplicationState(ApplicationState.RACK).value;
	}

	public String getDatacenter(InetAddress endpoint) {
		if (endpoint.equals(FBUtilities.getBroadcastAddress()))
			return ec2region;
		EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
		if (state == null || state.getApplicationState(ApplicationState.DC) == null) {
			if (savedEndpoints == null)
				savedEndpoints = SystemKeyspace.loadDcRackInfo();
			if (savedEndpoints.containsKey(endpoint))
				return savedEndpoints.get(endpoint).get("data_center");
			return DEFAULT_DC;
		}
		return state.getApplicationState(ApplicationState.DC).value;
	}
	
	@Override
    public void gossiperStarting()
    {
        super.gossiperStarting();
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP, StorageService.instance.valueFactory.internalIP(localPrivateAddress));
        Gossiper.instance.register(new ReconnectableSnitchHelper(this, ec2region, true));
    }
}
