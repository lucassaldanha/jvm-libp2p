package io.libp2p.pubsub.waggle

import io.libp2p.core.Connection
import io.libp2p.core.PeerId

/**
 * Application-defined description of which pieces of an object a node holds or wants.
 * Waggle never interprets the bytes.
 */
interface WaggleMetadata {
    /** Wire encoding, sent as `piecesMetadata`. */
    fun bytes(): ByteArray

    /**
     * Merges metadata received from a remote peer into this instance.
     * @return true if this instance changed
     * @throws Exception if [remote] is malformed
     */
    fun update(remote: ByteArray): Boolean
}

/**
 * An immutable snapshot of the pieces of one object held locally.
 * Publish a new snapshot after integrating new pieces; never mutate a published one.
 */
interface WaggleShard {
    /** Wire field `objectID`. */
    fun objectId(): ByteArray

    /** A fresh, independent metadata instance describing this snapshot. */
    fun metadata(): WaggleMetadata

    /**
     * Decides what to do for a peer given the metadata we track for it.
     * @param peerMetadata the peer's tracked metadata, or null if nothing is known yet
     * @throws Exception if [peerMetadata] is malformed
     */
    fun actionFor(peer: PeerId, connection: Connection, peerMetadata: ByteArray?): WaggleAction
}

/**
 * @param need the peer holds something this snapshot lacks
 * @param send pieces the peer lacks, or null when there is nothing to send
 */
class WaggleAction(val need: Boolean, val send: PieceSend?)

/**
 * @param pieces wire field `pieces`
 * @param peerMetadataAfter the peer's metadata once [pieces] are delivered
 */
class PieceSend(val pieces: ByteArray, val peerMetadataAfter: WaggleMetadata)

/** All callbacks run on the Waggle event thread. Hand expensive work off. */
interface WaggleEventListener {
    /** A peer sent something the application may need. Either of [metadata] and [pieces] may be null. */
    fun onReceived(
        peer: PeerId,
        connection: Connection,
        topic: ByteArray,
        objectId: ByteArray,
        metadata: ByteArray?,
        pieces: ByteArray?
    ) {}

    /** The remote subscribed to [topic] on [connection]: we can now send it objects. */
    fun onSubscribed(peer: PeerId, connection: Connection, topic: ByteArray) {}

    /** The remote closed its subscription to [topic] on [connection]. */
    fun onUnsubscribed(peer: PeerId, connection: Connection, topic: ByteArray) {}

    /** [WaggleMetadata.update] or [WaggleShard.actionFor] threw. Not necessarily the peer's fault. */
    fun onInvalidData(peer: PeerId, connection: Connection, topic: ByteArray, objectId: ByteArray, cause: Throwable) {}

    /** A reply to [peer] was dropped because the connection's outbound queue is full. */
    fun onFullQueue(peer: PeerId, connection: Connection) {}

    /** Our subscription stream for [topic] could not be opened, or was closed by the remote. Not retried. */
    fun onStreamFailed(peer: PeerId, connection: Connection, topic: ByteArray, cause: Throwable?) {}
}

data class WaggleTopicConfig(
    /** Connections that receive a published snapshot (successful sends, not attempts). */
    val publishFanout: Int = 50,
    /** Other connections that get a metadata-only advert when a peer tells us something new. */
    val gossipFanout: Int = 50
)

data class WaggleConfig(
    /** Maximum size of one protobuf frame. */
    val maxFrameSize: Int = 65536,
    /** Messages queued and not yet written, per connection, across all topics. */
    val maxQueuedMessagesPerConnection: Int = 5000,
    /** Inbound subscription streams per connection, counted from stream open. */
    val maxInboundStreamsPerConnection: Int = 50,
    /** Snapshots cached per topic (LRU). */
    val maxCachedObjectsPerTopic: Int = 100,
    val maxTopicIdSize: Int = 1024,
    val defaultTopicConfig: WaggleTopicConfig = WaggleTopicConfig(),
    val topicConfigs: Map<String, WaggleTopicConfig> = emptyMap()
)

class NoSubscribersException : RuntimeException("no peers subscribed to the topic")

/** Nothing was queued: [full] connections had a full queue, [noData] needed nothing new. */
class NotSentException(val full: Int, val noData: Int) :
    RuntimeException("not sent on any connection: $full full, $noData needed nothing new")
