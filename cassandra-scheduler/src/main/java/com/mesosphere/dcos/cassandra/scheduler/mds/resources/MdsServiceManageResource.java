package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

import java.net.InetSocketAddress;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.dcos.Capabilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.resources.ConnectionResource;

@Path("/v1/manage")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MdsServiceManageResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MdsServiceManageResource.class);
    private final ConfigurationManager configurationManager;
    private final Capabilities capabilities;
    private final CassandraState state;

    @Inject
    public MdsServiceManageResource(Capabilities capabilities, CassandraState state,
                    ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        this.capabilities = capabilities;
        this.state = state;
    }


    @POST
    @Path("/role/{rolename}")
    public Response addRole(@PathParam("rolename") final String rolename, RoleRequest roleRequest)
                    throws ConfigStoreException {
        try (Session session = getSession(roleRequest.getCassandraAuth())) {
            LOGGER.info("adding role:" + rolename + " role request:" + roleRequest);

            session.execute("CREATE ROLE " + rolename + " WITH PASSWORD = '" + roleRequest.getPassword()
                            + "' AND SUPERUSER = " + roleRequest.isSuperuser() + " AND LOGIN = " + roleRequest.isLogin()
                            + ";");
            if (roleRequest.isGrantAllPermissions()) {
                session.execute("GRANT ALL PERMISSIONS ON ALL KEYSPACES TO " + rolename + ";");
            }
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
        return Response.status(Response.Status.OK).entity("Successfull").build();

    }

    @PUT
    @Path("/role/{rolename}")
    public Response alterRole(@PathParam("rolename") final String rolename, RoleRequest roleRequest)
                    throws ConfigStoreException {
        LOGGER.info("alter role:" + rolename + " role request:" + roleRequest);

        try (Session session = getSession(roleRequest.getCassandraAuth())) {
            session.execute("ALTER ROLE " + rolename + " WITH PASSWORD = '" + roleRequest.getPassword()
                            + "' AND SUPERUSER = " + roleRequest.isSuperuser() + " AND LOGIN = " + roleRequest.isLogin()
                            + ";");
            if (roleRequest.isGrantAllPermissions()) {
                session.execute("GRANT ALL PERMISSIONS ON ALL KEYSPACES TO " + rolename + ";");
            }
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
        return Response.status(Response.Status.OK).entity("Successfull").build();
    }


    @PUT
    @Path("/keyspace/{keyspace}")
    public Response alterKeyspace(@PathParam("keyspace") final String keyspace,
                    AlterSystemAuthRequest alterSysteAuthRequest) throws ConfigStoreException {
        // Only used to alter system_auth RF for each region
        if (!keyspace.equalsIgnoreCase("system_auth")) {
            return Response.status(Response.Status.BAD_REQUEST)
                            .entity("Only system_auth key space is supported to alter").build();
        }

        try (Session session = getSession(alterSysteAuthRequest.getCassandraAuth())) {
            // session = getSession(alterSysteAuthRequest.getCassandraAuth());
            String dcRf = MdsCassandraUtills.getDataCenterVsReplicationFactorString(
                            alterSysteAuthRequest.getDataCenterVsReplicationFactor());
            String query = "ALTER KEYSPACE system_auth WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', " + dcRf
                            + "};";
            LOGGER.info("Alter system auth query:" + query);

            session.execute(query);
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }

        return Response.status(Response.Status.OK).entity("Successfull").build();
    }


    private Session getSession(CassandraAuth cassandraAuth) throws ConfigStoreException {
        final ConnectionResource connectionResource = new ConnectionResource(capabilities, state, configurationManager);
        List<String> connectedNodes = connectionResource.connectAddress();
        String conectionInfo = connectedNodes.get(0);
        String[] hostAndPort = conectionInfo.split(":");
        LOGGER.debug("connected node:" + hostAndPort);

        InetSocketAddress addresses = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        Cluster cluster = Cluster.builder().addContactPointsWithPorts(addresses)
                        .withCredentials(cassandraAuth.getUsername(), cassandraAuth.getPassword()).build();
        Session session = cluster.connect();
        return session;
    }
}