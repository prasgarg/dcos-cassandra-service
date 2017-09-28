package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.offer.VolumeRequirement.VolumeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

@Path("/v1/itest")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MdsItestManageResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(MdsItestManageResource.class);
	private final ConfigurationManager configurationManager;
	private final Capabilities capabilities;
	private final CassandraState state;

	@Inject
	public MdsItestManageResource(Capabilities capabilities, CassandraState state,
			ConfigurationManager configurationManager) {
		this.configurationManager = configurationManager;
		this.capabilities = capabilities;
		this.state = state;
	}

	@PUT
	@Path("/keyspace/{keyspace}")
	public Response alterKeyspace(@PathParam("keyspace") final String keyspace,
			AlterSystemAuthRequest alterSysteAuthRequest) throws ConfigStoreException {

		try (Session session = MdsCassandraUtills.getSession(alterSysteAuthRequest.getCassandraAuth(), capabilities, state, configurationManager)) {
			// session = getSession(alterSysteAuthRequest.getCassandraAuth());
			String dcRf = MdsCassandraUtills
					.getDataCenterVsReplicationFactorString(alterSysteAuthRequest.getDataCenterVsReplicationFactor());
			String query = "ALTER KEYSPACE " + keyspace + " WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', "
					+ dcRf + "};";
			LOGGER.info("Alter keyspace1 query:" + query);

			session.execute(query);
		} catch (NoHostAvailableException e) {
			return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(e.getMessage()).build();
		} catch (QueryExecutionException e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		} catch (QueryValidationException e) {
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}

		return Response.status(Response.Status.OK).entity("Successfull").build();
	}
	
	@GET
	@Path("/sstables/files")
	public SSTablesResponse getSSTablesFiles() throws IOException, InterruptedException {
		SSTablesResponse ssTablesResponse = new SSTablesResponse();
		VolumeType diskType = configurationManager.getTargetConfig().getCassandraConfig().getDiskType();
		String dataPath = "";
		if (VolumeType.MOUNT.equals(diskType)) {
			dataPath = "/dcos/volume1";
		} else {
			dataPath = "/var/lib/mesos/slave/volumes/roles/"
					+ configurationManager.getTargetConfig().getServiceConfig().getRole();
		}
		
		String command = "find " + dataPath + " -name *.db* |  rev | cut -d/ -f1 | rev";
		List<String> commands = new ArrayList<String>();
		commands.add("/bin/sh");
		commands.add("-c");
		commands.add(command);
		Runtime run = Runtime.getRuntime();
		Process pr = run.exec(commands.toArray(new String[commands.size()]));
		pr.waitFor();
		BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
		String line = "";
		while ((line = buf.readLine()) != null) {
			ssTablesResponse.addSSTablesFileName(line);
		}
		return ssTablesResponse;
	}

	
}
