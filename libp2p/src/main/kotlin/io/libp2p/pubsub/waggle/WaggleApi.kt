package io.libp2p.pubsub.waggle

import io.libp2p.core.Connection
import io.libp2p.core.PeerId
import java.time.Duration

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
 *
 * Snapshots of an object must only grow. Waggle remembers which pieces it has sent to each peer
 * and never sends them again, so publishing a snapshot that lacks pieces an earlier one had
 * leaves peers believing we still hold them.
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
    fun actionFor(peer: PeerId, peerMetadata: ByteArray?): WaggleAction
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

    /**
     * Our subscription stream for [topic] could not be opened ([cause] is set; typically the peer
     * does not speak Waggle; never retried) or was closed by the remote ([cause] is null).
     * @param willRetry the stream will be reopened after a backoff
     */
    fun onStreamFailed(peer: PeerId, connection: Connection, topic: ByteArray, cause: Throwable?, willRetry: Boolean) {}

    /**
     * Every ObjectPieces frame read from a peer, before Waggle decides whether it is useful.
     * Meant for metrics and for scoring peers that send frames which never lead to [onReceived].
     * @param metadataSize size of `piecesMetadata`, or -1 if absent
     * @param piecesSize size of `pieces`, or -1 if absent
     * @param dataStream the frame arrived on a data stream (go-waggle extension)
     */
    fun onFrameReceived(
        peer: PeerId,
        connection: Connection,
        topic: ByteArray,
        objectId: ByteArray,
        metadataSize: Int,
        piecesSize: Int,
        dataStream: Boolean
    ) {}

    /** Every ObjectPieces frame admitted to a connection's queue. Sizes as in [onFrameReceived]. */
    fun onFrameSent(
        peer: PeerId,
        connection: Connection,
        topic: ByteArray,
        objectId: ByteArray,
        metadataSize: Int,
        piecesSize: Int,
        dataStream: Boolean
    ) {}
}

/** Receives every message of a topic subscribed with [Waggle.subscribeRaw]. Called on the event thread. */
fun interface WaggleRawHandler {
    fun onMessage(peer: PeerId, connection: Connection, objectId: ByteArray, metadata: ByteArray?, pieces: ByteArray?)
}

data class WaggleTopicConfig(
    /** Peers we know nothing about that receive a published snapshot (successful sends, not attempts). */
    val publishFanout: Int = 50,
    /** Peers whose view of the object we already track that receive what they lack on publish. */
    val forwardFanout: Int = 50,
    /** Other peers that get a metadata-only advert when a peer tells us something new. */
    val gossipFanout: Int = 50,
    /**
     * Also open a separate data stream, so the remote can keep metadata ahead of payload
     * (go-waggle extension, not in the spec draft). Peers that predate it reject the second
     * stream and keep sending everything on the first.
     */
    val dataStream: Boolean = false
)

data class WaggleConfig(
    /** Maximum size of one protobuf frame. */
    val maxFrameSize: Int = 65536,
    /** Messages handed to a connection and not yet written, across all its topics. */
    val maxQueuedMessagesPerConnection: Int = 5000,
    /** Bytes handed to a connection and not yet written, across all its topics. */
    val maxQueuedBytesPerConnection: Long = 4L * 1024 * 1024,
    /** Inbound subscription streams per connection, counted from stream open. */
    val maxInboundStreamsPerConnection: Int = 50,
    /** Snapshots cached per topic (LRU). */
    val maxCachedObjectsPerTopic: Int = 100,
    /** A snapshot expires this long after it was last published; a peer view this long after it last changed. */
    val objectLifetime: Duration = Duration.ofMinutes(5),
    val maxTopicIdSize: Int = 1024,
    /** An inbound stream that has not sent its TopicSubscription after this long is closed. */
    val handshakeTimeout: Duration = Duration.ofSeconds(5),
    /** Delay before the first attempt to reopen a subscription stream the remote closed; doubles per attempt. */
    val reopenInitialBackoff: Duration = Duration.ofSeconds(1),
    val reopenMaxBackoff: Duration = Duration.ofSeconds(60),
    /** Consecutive reopen attempts without receiving anything before giving up on a stream. */
    val maxReopenAttempts: Int = 5,
    /** Inbound subscriptions to topics this rejects are closed. Called on the event thread. */
    val topicFilter: (ByteArray) -> Boolean = { true },
    val defaultTopicConfig: WaggleTopicConfig = WaggleTopicConfig(),
    val topicConfigs: Map<String, WaggleTopicConfig> = emptyMap()
)

/** The peer holds no subscription to the topic with us. */
class PeerNotSubscribedException : RuntimeException("the peer is not subscribed to the topic")

/** The connection's outbound queue is full; the message was not queued. */
class QueueFullException : RuntimeException("the connection's outbound queue is full")

class NoSubscribersException : RuntimeException("no peers subscribed to the topic")

/** Nothing was queued: [full] connections had a full queue, [noData] needed nothing new. */
class NotSentException(val full: Int, val noData: Int) :
    RuntimeException("not sent on any connection: $full full, $noData needed nothing new")
