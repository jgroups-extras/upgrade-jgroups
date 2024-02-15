package org.jgroups.protocols;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import org.jgroups.Address;
import org.jgroups.BaseMessage;
import org.jgroups.BytesMessage;
import org.jgroups.Message;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.rolling_upgrades.*;
import org.jgroups.rolling_upgrades.Message.Builder;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Util;

import java.util.*;
import java.util.stream.Collectors;

import static org.jgroups.rolling_upgrades.RelayHeader.Type.*;

/**
 * Relays application messages to the UpgradeServer (when active). Should be the top protocol in a stack.
 * @author Bela Ban
 * @since  1.0
 */
public class UPGRADE extends UpgradeBase {
    protected static final Map<RelayHeader.Type,Byte> RELAY_TYPES_1=Map.of(
      DATA,             org.jgroups.protocols.relay.RelayHeader.DATA,
      SITE_UNREACHABLE, org.jgroups.protocols.relay.RelayHeader.SITE_UNREACHABLE,
      SITES_UP,         org.jgroups.protocols.relay.RelayHeader.SITES_UP,
      SITES_DOWN,       org.jgroups.protocols.relay.RelayHeader.SITES_DOWN,
      TOPO_REQ,         org.jgroups.protocols.relay.RelayHeader.TOPO_REQ,
      TOPO_RSP,         org.jgroups.protocols.relay.RelayHeader.TOPO_RSP);

    protected static final Map<Byte,RelayHeader.Type> RELAY_TYPES_2=Map.of(
      org.jgroups.protocols.relay.RelayHeader.DATA, DATA,
      org.jgroups.protocols.relay.RelayHeader.SITE_UNREACHABLE, SITE_UNREACHABLE,
      org.jgroups.protocols.relay.RelayHeader.SITES_UP, SITES_UP,
      org.jgroups.protocols.relay.RelayHeader.SITES_DOWN, SITES_DOWN,
      org.jgroups.protocols.relay.RelayHeader.TOPO_REQ, TOPO_REQ,
      org.jgroups.protocols.relay.RelayHeader.TOPO_RSP, TOPO_RSP);

    protected static final Map<UnicastHeader.Type,Byte> UNICAST_1=Map.of(
      UnicastHeader.Type.DATA, UnicastHeader3.DATA,
      UnicastHeader.Type.ACK, UnicastHeader3.ACK,
      UnicastHeader.Type.SEND_FIRST_SEQNO, UnicastHeader3.SEND_FIRST_SEQNO,
      UnicastHeader.Type.XMIT_REQ, UnicastHeader3.XMIT_REQ,
      UnicastHeader.Type.CLOSE, UnicastHeader3.CLOSE
    );

    protected static final Map<Byte,UnicastHeader.Type> UNICAST_2=Map.of(
      UnicastHeader3.DATA, UnicastHeader.Type.DATA,
      UnicastHeader3.ACK, UnicastHeader.Type.ACK,
      UnicastHeader3.SEND_FIRST_SEQNO, UnicastHeader.Type.SEND_FIRST_SEQNO,
      UnicastHeader3.XMIT_REQ, UnicastHeader.Type.XMIT_REQ,
      UnicastHeader3.CLOSE, UnicastHeader.Type.CLOSE
    );

    @Override
    protected org.jgroups.rolling_upgrades.Message jgMessageToProtoMessage(String cluster, Message jg_msg) throws Exception {
        Metadata md=Metadata.newBuilder().setMsgType(jg_msg.getType()).build();
        // Sets cluster name, destination and sender addresses, flags and metadata (e.g. message type)
        Builder builder=msgBuilder(cluster, jg_msg.getSrc(), jg_msg.getDest(), jg_msg.getFlags(), md);

        // Sets the headers
        List<Header> proto_hdrs=jgHeadersToProtoHeaders(((BaseMessage)jg_msg).headers(), jg_msg);
        builder.addAllHeaders(proto_hdrs);
        boolean is_rsp=proto_hdrs != null && proto_hdrs.stream()
          .anyMatch(h -> h.hasRpcHdr() && h.getRpcHdr().getType() > 0); // 1=RSP, 2=EXC_RSP

        // Sets the payload
        org.jgroups.rolling_upgrades.ByteArray payload;
        if((is_rsp || rpcs) && marshaller != null) {
            Object obj=jg_msg.getPayload();
            payload=marshaller.objectToBuffer(obj);
        }
        else {
            if(jg_msg.hasArray())
                payload=new ByteArray(jg_msg.getArray(), jg_msg.getOffset(), jg_msg.getLength());
            else {
                // todo: objectToByteBuffer()/Message.getObject() are not JGroups version-independent!
                org.jgroups.util.ByteArray pl=jg_msg.hasPayload()? Util.objectToBuffer(jg_msg.getObject()) : null;
                payload=pl != null? new ByteArray(pl.getArray(), pl.getOffset(), pl.getLength()) : null;
            }
        }
        if(payload != null)
            builder.setPayload(ByteString.copyFrom(payload.getBytes(), payload.getOffset(), payload.getLength()));
        return builder.build();
    }


    @Override
    protected Message protoMessageToJGMessage(org.jgroups.rolling_upgrades.Message msg) throws Exception {
        ByteString payload=msg.getPayload();
        Message jg_msg=msg.hasMetaData()? msg_factory.create((short)msg.getMetaData().getMsgType())
          : new BytesMessage();
        if(msg.hasDestination())
            jg_msg.setDest(protoAddressToJGAddress(msg.getDestination()));
        if(msg.hasSender())
            jg_msg.setSrc(protoAddressToJGAddress(msg.getSender()));
        jg_msg.setFlag((short)msg.getFlags(), false);
        boolean is_rsp=false;

        // Headers
        List<Header> headers=msg.getHeadersList();
        if(headers != null) {
            is_rsp=headers.stream().anyMatch(h -> h.hasRpcHdr() && h.getRpcHdr().getType() > 0); // 1=RSP, 2=EXC_RSP
            org.jgroups.Header[] hdrs=protoHeadersToJGHeaders(headers, jg_msg);
            if(hdrs != null)
                ((BaseMessage)jg_msg).headers(hdrs);
        }

        // Payload
        if(!payload.isEmpty()) {
            byte[] tmp=payload.toByteArray();
            if((is_rsp || rpcs) && marshaller != null) {
                Object obj=marshaller.objectFromBuffer(tmp, 0, tmp.length);
                jg_msg.setPayload(obj);
            }
            else {
                if(jg_msg.hasArray())
                    jg_msg.setArray(tmp);
                else {
                    Object pl=Util.objectFromByteBuffer(tmp); // this is NOT compatible between different versions!
                    jg_msg.setObject(pl);
                }
            }
        }
        return jg_msg;
    }


    protected List<Header> jgHeadersToProtoHeaders(org.jgroups.Header[] jg_hdrs, Message jg_msg) {
        if(jg_hdrs == null || jg_hdrs.length == 0)
            return List.of();
        List<Header> l=new ArrayList<>(jg_hdrs.length);
        for(org.jgroups.Header h: jg_hdrs) {
            Header hdr=null;
            if(h instanceof RequestCorrelator.Header) {
                RpcHeader rpc_hdr=jgRpcHeaderToProto((RequestCorrelator.Header)h);
                hdr=Header.newBuilder().setRpcHdr(rpc_hdr).setProtocolId(h.getProtId()).build();
            }
            else if(h instanceof org.jgroups.protocols.relay.RelayHeader) {
                RelayHeader rh=jgRelayHeaderToProto((org.jgroups.protocols.relay.RelayHeader)h);
                if(rh != null)
                    hdr=Header.newBuilder().setRelayHdr(rh).setProtocolId(h.getProtId()).build();
            }
            else if(h instanceof FORK.ForkHeader) {
                ForkHeader fh=jgForkHeaderToProto((org.jgroups.protocols.FORK.ForkHeader)h);
                hdr=Header.newBuilder().setForkHdr(fh).setProtocolId(h.getProtId()).build();
            }
            else if(h instanceof UnicastHeader3) {
                UnicastHeader uh=jgUnicastHeaderToProto((UnicastHeader3)h, jg_msg);
                hdr=Header.newBuilder().setUnicastHdr(uh).setProtocolId(h.getProtId()).build();
            }
            if(hdr != null)
                l.add(hdr);
        }
        return l;
    }

    protected org.jgroups.Header[] protoHeadersToJGHeaders(List<Header> proto_hdrs, org.jgroups.Message msg) {
        if(proto_hdrs == null || proto_hdrs.isEmpty())
            return null;
        org.jgroups.Header[] retval=new org.jgroups.Header[proto_hdrs.size()];
        int index=0;
        for(Header h: proto_hdrs) {
            if(h.hasRpcHdr())
                retval[index++]=protoRpcHeaderToJG(h.getRpcHdr()).setProtId((short)h.getProtocolId());
            else if(h.hasRelayHdr()) {
                org.jgroups.Header hdr=protoRelayHeaderToJG(h.getRelayHdr()).setProtId((short)h.getProtocolId());
                if(hdr != null)
                    retval[index++]=hdr;
            }
            else if(h.hasForkHdr())
                retval[index++]=protoForkHeaderToJG(h.getForkHdr()).setProtId((short)h.getProtocolId());
            else if(h.hasUnicastHdr()) // only needed with RELAY3; with RELAY2, UNICAST3 is always below it!
                retval[index++]=protoUnicastHeaderToJG(h.getUnicastHdr(), msg).setProtId((short)h.getProtocolId());
        }
        return retval;
    }

    protected static RpcHeader jgRpcHeaderToProto(RequestCorrelator.Header hdr) {
        RpcHeader.Builder builder = RpcHeader.newBuilder().setType(hdr.type).setRequestId(hdr.req_id).setCorrId(hdr.corrId);
        if (hdr instanceof RequestCorrelator.MultiDestinationHeader) {
            RequestCorrelator.MultiDestinationHeader mdhdr = (RequestCorrelator.MultiDestinationHeader) hdr;
            Address[] exclusions=mdhdr.exclusion_list;
            if(exclusions != null && exclusions.length > 0) {
                builder.addAllExclusionList(Arrays.stream(exclusions).map(UpgradeBase::jgAddressToProtoAddress)
                                              .collect(Collectors.toList()));
            }
        }
        return builder.build();
    }

    protected static RequestCorrelator.Header protoRpcHeaderToJG(RpcHeader hdr) {
        return new RequestCorrelator.Header((byte)hdr.getType(), hdr.getRequestId(), (short)hdr.getCorrId());
    }

    protected RelayHeader jgRelayHeaderToProto(org.jgroups.protocols.relay.RelayHeader jg_hdr) {
        RelayHeader.Type type=RELAY_TYPES_2.get(jg_hdr.getType());
        if(type == null) {
            log.error("type %s not recognized; dropping RelayHeader", type);
            return null;
        }
        RelayHeader.Builder rb=RelayHeader.newBuilder().setType(type);
        if(jg_hdr.getFinalDest() != null) {
            org.jgroups.rolling_upgrades.Address addr=jgAddressToProtoAddress(jg_hdr.getFinalDest());
            rb.setFinalDest(addr);
        }
        if(jg_hdr.getOriginalSender() != null) {
            org.jgroups.rolling_upgrades.Address addr=jgAddressToProtoAddress(jg_hdr.getOriginalSender());
            rb.setOriginalSender(addr);
        }
        Set<String> sites=jg_hdr.getSites();
        if(sites != null && !sites.isEmpty())
            rb.addAllSites(sites);
        return rb.build();
    }

    protected org.jgroups.Header protoRelayHeaderToJG(RelayHeader pbuf_hdr) {
        Address     final_dest=null, original_sender=null;
        Set<String> sites=null;
        RelayHeader.Type pbuf_type=pbuf_hdr.getType();

        Byte type=RELAY_TYPES_1.get(pbuf_type);
        if(type == null) {
            log.error("type %d is unrecognized; dropping RelayHeader", type);
        }
        if(pbuf_hdr.hasFinalDest())
            final_dest=protoAddressToJGAddress(pbuf_hdr.getFinalDest());
        if(pbuf_hdr.hasOriginalSender())
            original_sender=protoAddressToJGAddress(pbuf_hdr.getOriginalSender());
        ProtocolStringList pbuf_sites=pbuf_hdr.getSitesList();
        if(pbuf_sites != null)
            sites=new HashSet<>(pbuf_sites);
        org.jgroups.protocols.relay.RelayHeader hdr=new org.jgroups.protocols.relay.RelayHeader(type, final_dest, original_sender);
        if(sites != null)
            hdr.addToSites(sites);
        return hdr;
    }

    protected static ForkHeader jgForkHeaderToProto(FORK.ForkHeader h) {
        return ForkHeader.newBuilder().setForkStackId(h.getForkStackId()).setForkChannelId(h.getForkChannelId()).build();
    }

    protected static org.jgroups.Header protoForkHeaderToJG(ForkHeader h) {
        return new FORK.ForkHeader(h.getForkStackId(), h.getForkChannelId());
    }

    protected static org.jgroups.Header protoUnicastHeaderToJG(UnicastHeader h, Message msg) {
        Byte type=UNICAST_1.get(h.getType());
        if(type == null)
            throw new IllegalArgumentException(String.format("type %d is unrecognized", type));
        UnicastHeader3 hdr=new UnicastHeader3(type, h.getSeqno(), (short)h.getConnId(), h.getFirst())
          .timestamp(h.getTimestamp());

        // for XMIT_REQ, the missing seqnos are in the payload
        if(h.getType() == UnicastHeader.Type.XMIT_REQ) {
            List<Long> seqnos_to_xmit=h.getMissingSeqnosList();
            if(seqnos_to_xmit != null && !seqnos_to_xmit.isEmpty()) {
                long lowest=seqnos_to_xmit.stream().min(Long::compareTo).get();
                SeqnoList seqnos=new SeqnoList(seqnos_to_xmit.size(), lowest);
                msg.setObject(seqnos);
            }
        }
        return hdr;
    }

    protected static UnicastHeader jgUnicastHeaderToProto(UnicastHeader3 h, Message jg_msg) {
        UnicastHeader.Type type=UNICAST_2.get(h.type());
        if(type == null)
            throw new IllegalArgumentException(String.format("type %s is unrecognized", type));
        UnicastHeader.Builder builder=UnicastHeader.newBuilder().setType(type).setSeqno(h.seqno()).setConnId(h.connId())
          .setFirst(h.first()).setTimestamp(h.timestamp());

        // for XMIT_REQ, the missing seqnos are in the payload
        if(type == UnicastHeader.Type.XMIT_REQ) {
            SeqnoList seqnos=jg_msg.getObject();
            if(seqnos != null && !seqnos.isEmpty())
                builder.addAllMissingSeqnos(seqnos);
        }
        return builder.build();
    }

    protected static Builder msgBuilder(String cluster, Address src, Address dest, short flags, Metadata md) {
        Builder builder=org.jgroups.rolling_upgrades.Message.newBuilder().setClusterName(cluster);
        if(dest !=null)
            builder.setDestination(jgAddressToProtoAddress(dest));
        if(src != null)
            builder.setSender(jgAddressToProtoAddress(src));
        if(md != null)
            builder.setMetaData(md);
        return builder.setFlags(flags);
    }



}
