package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.rolling_upgrades.ConnectionStatus;
import org.jgroups.rolling_upgrades.Marshaller;
import org.jgroups.rolling_upgrades.Request;
import org.jgroups.rolling_upgrades.UpgradeClient;
import org.jgroups.stack.Protocol;
import org.jgroups.util.NameCache;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.jgroups.rolling_upgrades.ConnectionStatus.State.connected;
import static org.jgroups.rolling_upgrades.ConnectionStatus.State.disconnecting;


/**
 * Relays application messages to the UpgradeServer (when active). Should be the top protocol in a stack.
 * @author Bela Ban
 * @since  1.1.1
 * @todo: implement support for addresses other than UUIDs
 * @todo: implement reconnection to server (server went down and then up again)
 */
@MBean(description="Protocol that redirects all messages to/from an UpgradeServer")
public abstract class UpgradeBase extends Protocol {
    @Property(description="Whether or not to perform relaying via the UpgradeServer", writable=false)
    protected volatile boolean   active;

    @Property(description="The IP address (or symbolic name) of the UpgradeServer")
    protected String             server_address="localhost";

    @Property(description="The port on which the UpgradeServer is listening")
    protected int                server_port=50051;

    @Property(description="The filename of the UpgradeServer's certificate (with the server's public key). " +
      "If non-null and non-empty, the client will use an encrypted connection to the server")
    protected String             server_cert;

    @Property(description="Time in ms between trying to reconnect to UpgradeServer (while disconnected)")
    protected long               reconnect_interval=3000;

    @ManagedAttribute(description="Shows the local view")
    protected View               local_view;

    @ManagedAttribute(description="The global view (provided by the UpgradeServer)")
    protected View               global_view;

    @Property(description="If RPCs are sent over UPGRADE, then we must serialize every request, not just the responses")
    protected boolean            rpcs;

    @ManagedAttribute(description="The cluster this member is a part of")
    protected String             cluster;

    protected UpgradeClient      client=new UpgradeClient();

    protected Marshaller         marshaller;

    protected MessageFactory     msg_factory;


    protected abstract org.jgroups.rolling_upgrades.Message jgMessageToProtoMessage(String cluster, Message jg_msg)
      throws Exception;
    protected abstract Message protoMessageToJGMessage(org.jgroups.rolling_upgrades.Message proto_msg) throws Exception;


    @ManagedAttribute
    public String getMarshaller() {
        return marshaller != null? marshaller.getClass().getSimpleName() : "n/a";
    }

    public Marshaller marshaller() {
        return marshaller;
    }

    public <T extends UpgradeBase> T marshaller(Marshaller m) {
        this.marshaller=m;
        return (T)this;
    }

    public boolean getRpcs() {
        return rpcs;
    }

    public <T extends UpgradeBase> T setRpcs(boolean r) {
        rpcs=r;
        return (T)this;
    }

    @ManagedAttribute(description="True if the reconnector is running")
    public boolean isReconnecting() {
        return client.reconnectorRunning();
    }

    @ManagedAttribute(description="state of the connection")
    public ConnectionStatus state() {return client.state();}

    @ManagedAttribute(description="Whether or not this member is the coordinator")
    public boolean isCoordinator() {
        return Objects.equals(local_addr, local_view != null? local_view.getCreator() : null);
    }

    public void init() throws Exception {
        super.init();
        msg_factory=getTransport().getMessageFactory();
        client.serverAddress(server_address).serverPort(server_port).serverCert(server_cert)
          .addViewHandler(this::handleView).addMessageHandler(this::handleMessage)
          .viewResponseHandler(this::handleViewResponse)
          .reconnectionFunction(this::connect).reconnectInterval(reconnect_interval)
          .start();
    }

    public void stop() {
        client.stop();
    }

    @ManagedOperation(description="Enable forwarding and receiving of messages to/from the UpgradeServer")
    public synchronized void activate() {
        if(!active) {
            registerView();
            active=true;
        }
    }

    @ManagedOperation(description="Activates only if cluster_name matches the local cluster")
    public void activate(String cluster_name) {
        if(cluster != null && Objects.equals(cluster, cluster_name))
            activate();
    }

    @ManagedOperation(description="Disable forwarding and receiving of messages to/from the UpgradeServer")
    public synchronized void deactivate() {
        if(active) {
            state().setState(connected, disconnecting);
            active=false;
            getViewFromServer();
        }
    }

    @ManagedOperation(description="Deactivates only if cluster_name matches the local cluster")
    public void deactivate(String cluster_name) {
        if(cluster != null && Objects.equals(cluster, cluster_name))
            deactivate();
    }

    public Object down(Event evt) {
        switch(evt.type()) {
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster=evt.arg();
                // todo: check if View is relayed to all members, this would be incompatible!
                Object ret=down_prot.down(evt);
                if(active)
                    connect();
                return ret;
            case Event.DISCONNECT:
                ret=down_prot.down(evt);
                if(active)
                    disconnect();
                return ret;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        if(evt.type() == Event.VIEW_CHANGE) {
            local_view=evt.arg();
            if(active)
                return null;
        }
        return up_prot.up(evt);
    }

    public Object down(Message msg) {
        if(!active)
            return down_prot.down(msg);

        // else send to UpgradeServer
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
        try {
            org.jgroups.rolling_upgrades.Message m=jgMessageToProtoMessage(cluster, msg);
            Request req=Request.newBuilder().setMessage(m).build();
            client.send(req);
        }
        catch(Exception e) {
            throw new RuntimeException(String.format("%s: failed sending message: %s", local_addr, e));
        }
        return null;
    }

    protected void registerView() {
        org.jgroups.rolling_upgrades.View v=jgViewToProtoView(local_view);
        org.jgroups.rolling_upgrades.Address local=jgAddressToProtoAddress(local_addr);
        log.debug("%s: registering view %s", local_addr, local_view);
        client.registerView(cluster, v, local);
    }

    protected void getViewFromServer() {
        log.debug("%s: getting view for cluster %s", local_addr, cluster);
        client.getViewFromServer(cluster);
    }

    protected void connect() {
        org.jgroups.rolling_upgrades.Address addr=jgAddressToProtoAddress(local_addr);
        log.debug("%s: joining cluster %s", local_addr, cluster);
        client.connect(cluster, addr);
    }

    protected void disconnect() {
        org.jgroups.rolling_upgrades.Address addr=jgAddressToProtoAddress(local_addr);
        log.debug("%s: leaving cluster %s", local_addr, cluster);
        client.disconnect(cluster, addr);
    }

    protected void handleView(org.jgroups.rolling_upgrades.View view) {
        View jg_view=protoViewToJGView(view);
        if(!active) {
            log.warn("%s: global view %s from server is discarded as active == false", local_addr, jg_view);
            return;
        }
        global_view=jg_view;
        log.debug("%s: received new global view %s", local_addr, global_view);
        up_prot.up(new Event(Event.VIEW_CHANGE, jg_view));
    }

    protected void handleMessage(org.jgroups.rolling_upgrades.Message m) {
        try {
            Message msg=protoMessageToJGMessage(m);
            up_prot.up(msg);
        }
        catch(Exception e) {
            log.error("%s: failed reading message: %s", local_addr, e);
        }
    }

    protected void handleViewResponse(org.jgroups.rolling_upgrades.GetViewResponse rsp) {
        org.jgroups.rolling_upgrades.View v=rsp.getView();
        View view=protoViewToJGView(v);

        // Install a MergeView *if* I'm the coordinator of the global view
        if(Objects.equals(local_addr, view.getCreator())) {
            long view_id=Math.max(view.getViewId().getId(), local_view == null? 1 : local_view.getViewId().getId()) +1;
            MergeView mv=new MergeView(view.getCreator(), view_id, view.getMembers(), List.of(view));
            log.debug("%s: I'm the coordinator, installing new local view %s", local_addr, mv);
            GMS gms=stack.findProtocol(GMS.class);
            gms.castViewChangeAndSendJoinRsps(mv, null, mv.getMembers(), null, null);
        }
        else
            log.debug("%s: I'm not coordinator, waiting for new MergeView from global view %s", local_addr, view);
        active=false;
    }


    protected static org.jgroups.rolling_upgrades.Address jgAddressToProtoAddress(Address jgroups_addr) {
        if(jgroups_addr == null)
            return org.jgroups.rolling_upgrades.Address.newBuilder().build();
        if(!(jgroups_addr instanceof org.jgroups.util.UUID))
            throw new IllegalArgumentException(String.format("JGroups address has to be of type UUID but is %s",
                                                             jgroups_addr.getClass().getSimpleName()));
        org.jgroups.util.UUID uuid=(org.jgroups.util.UUID)jgroups_addr;
        String name=jgroups_addr instanceof SiteUUID? ((SiteUUID)jgroups_addr).getName() : NameCache.get(jgroups_addr);

        org.jgroups.rolling_upgrades.Address.Builder addr_builder=org.jgroups.rolling_upgrades.Address.newBuilder();
        org.jgroups.rolling_upgrades.UUID pbuf_uuid=org.jgroups.rolling_upgrades.UUID.newBuilder()
          .setLeastSig(uuid.getLeastSignificantBits()).setMostSig(uuid.getMostSignificantBits()).build();

        if(jgroups_addr instanceof SiteUUID || jgroups_addr instanceof SiteMaster) {
            String site_name=((SiteUUID)jgroups_addr).getSite();
            org.jgroups.rolling_upgrades.SiteUUID.Builder b=org.jgroups.rolling_upgrades.SiteUUID.newBuilder().
              setUuid(pbuf_uuid);
            if(site_name != null)
                b.setSiteName(site_name);
            if(jgroups_addr instanceof SiteMaster)
                b.setIsSiteMaster(true);
            addr_builder.setSiteUuid(b.build());
        }
        else {
            addr_builder.setUuid(pbuf_uuid);
        }
        if(name != null)
            addr_builder.setName(name);
        return addr_builder.build();
    }

    protected static Address protoAddressToJGAddress(org.jgroups.rolling_upgrades.Address pbuf_addr) {
        if(pbuf_addr == null)
            return null;

        String logical_name=pbuf_addr.getName();
        Address retval=null;
        if(pbuf_addr.hasSiteUuid()) {
            org.jgroups.rolling_upgrades.SiteUUID pbuf_site_uuid=pbuf_addr.getSiteUuid();
            String site_name=pbuf_site_uuid.getSiteName();
            if(pbuf_site_uuid.getIsSiteMaster())
                retval=new SiteMaster(site_name);
            else {
                long least=pbuf_site_uuid.getUuid().getLeastSig(), most=pbuf_site_uuid.getUuid().getMostSig();
                retval=new SiteUUID(most, least, logical_name, site_name);
            }
        }
        else if(pbuf_addr.hasUuid()) {
            org.jgroups.rolling_upgrades.UUID pbuf_uuid=pbuf_addr.getUuid();
            retval=new org.jgroups.util.UUID(pbuf_uuid.getMostSig(), pbuf_uuid.getLeastSig());
        }

        if(retval != null && logical_name != null && !logical_name.isEmpty())
            NameCache.add(retval, logical_name);
        return retval;
    }

    protected static org.jgroups.rolling_upgrades.View jgViewToProtoView(View v) {
        org.jgroups.rolling_upgrades.ViewId view_id=jgViewIdToProtoViewId(v.getViewId());
        List<org.jgroups.rolling_upgrades.Address> mbrs=new ArrayList<>(v.size());
        for(Address a: v)
            mbrs.add(jgAddressToProtoAddress(a));
        return org.jgroups.rolling_upgrades.View.newBuilder().addAllMember(mbrs).setViewId(view_id).build();
    }

    protected static org.jgroups.rolling_upgrades.ViewId jgViewIdToProtoViewId(org.jgroups.ViewId view_id) {
        org.jgroups.rolling_upgrades.Address coord=jgAddressToProtoAddress(view_id.getCreator());
        return org.jgroups.rolling_upgrades.ViewId.newBuilder().setCreator(coord).setId(view_id.getId()).build();
    }

    protected static org.jgroups.View protoViewToJGView(org.jgroups.rolling_upgrades.View v) {
        org.jgroups.rolling_upgrades.ViewId vid=v.getViewId();
        List<org.jgroups.rolling_upgrades.Address> pbuf_mbrs=v.getMemberList();
        org.jgroups.ViewId jg_vid=new org.jgroups.ViewId(protoAddressToJGAddress(vid.getCreator()), vid.getId());
        List<Address> members=pbuf_mbrs.stream().map(UpgradeBase::protoAddressToJGAddress).collect(Collectors.toList());
        return new org.jgroups.View(jg_vid, members);
    }


}
