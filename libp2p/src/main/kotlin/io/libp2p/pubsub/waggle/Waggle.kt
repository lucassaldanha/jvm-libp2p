package io.libp2p.pubsub.waggle

import com.google.protobuf.ByteString
import com.google.protobuf.MessageLite
import io.libp2p.core.Connection
import io.libp2p.core.ConnectionHandler
import io.libp2p.core.P2PChannel
import io.libp2p.core.PeerId
import io.libp2p.core.Stream
import io.libp2p.core.multistream.ProtocolBinding
import io.libp2p.core.multistream.ProtocolDescriptor
import io.libp2p.etc.types.WBytes
import io.libp2p.etc.util.netty.mux.RemoteWriteClosed
import io.libp2p.etc.util.netty.protobuf.LimitedProtobufVarint32FrameDecoder
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelPromise
import io.netty.handler.codec.protobuf.ProtobufDecoder
import io.netty.handler.codec.protobuf.ProtobufEncoder
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender
import org.slf4j.LoggerFactory
import waggle.pb.WaggleRpc.ObjectPieces
import waggle.pb.WaggleRpc.TopicSubscription
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

const val WAGGLE_PROTOCOL_ID = "/waggle/1.0"

/** Stream kinds of the go-waggle handshake extension. */
private const val CONTROL_STREAM = 0
private const val DATA_STREAM = 1

/**
 * Prototype of the Waggle pubsub protocol (libp2p/specs#732, r1 draft).
 *
 * Register it on a Host with `protocols { +waggle }`. It is a sibling of Gossip, not part of it:
 * one muxer stream per (connection, topic, direction), no mesh, no message ids, no scoring.
 *
 * Stream model: to subscribe we open a stream, write one [TopicSubscription] and then only read
 * [ObjectPieces]. The remote does the same towards us, and that inbound stream is the one we
 * write the topic's [ObjectPieces] to. Closing a stream unsubscribes.
 *
 * Peers, not connections, are the unit of reconciliation. We subscribe on one connection per peer
 * (the primary; another takes over when it closes) and accept subscriptions on any connection.
 * This suits both per-connection implementations (Rust, Nim) and per-peer ones (Go, which resets
 * an older stream when a second one arrives for the same peer and topic).
 *
 * Flow control: a message counts against its connection's budget (messages and bytes) from the
 * moment it is handed to the stream until the write completes; over budget, sends fail fast. This
 * bounds memory wherever the stream reports real write progress. Yamux and Mplex streams in
 * jvm-libp2p complete writes as soon as the muxer has buffered them, so there the muxer's own
 * buffer limit (which closes the connection when exceeded) is the effective bound.
 *
 * All state lives on a single event thread; listeners are called on it.
 */
class Waggle @JvmOverloads constructor(
    private val config: WaggleConfig = WaggleConfig(),
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "waggle-event-thread").also { it.isDaemon = true }
    },
    private val random: Random = Random(),
    clock: () -> Long = System::currentTimeMillis
) : ProtocolBinding<Unit>, ConnectionHandler {

    private val logger = LoggerFactory.getLogger(Waggle::class.java)

    override val protocolDescriptor = ProtocolDescriptor(WAGGLE_PROTOCOL_ID)

    private val listeners = CopyOnWriteArrayList<WaggleEventListener>()

    private data class StreamKey(val topic: WBytes, val kind: Int)

    private class PeerState(val peerId: PeerId) {
        val connections = ArrayList<ConnState>()

        /** The connection our own subscription streams are on. */
        var primary: ConnState? = null

        /** The stream to write [key]'s messages to: on the primary connection if the remote subscribed there. */
        fun sendStream(key: StreamKey): Pair<ConnState, Stream>? {
            primary?.also { p -> p.sendStreams[key]?.also { return p to it } }
            for (cs in connections) cs.sendStreams[key]?.also { return cs to it }
            return null
        }

        fun subscribedTo(topic: WBytes) = sendStream(StreamKey(topic, CONTROL_STREAM)) != null
    }

    private class ConnState(val connection: Connection, val peer: PeerState) {
        /** Streams the remote opened: we write ObjectPieces to them. */
        val sendStreams = HashMap<StreamKey, Stream>()

        /** Streams we opened: we read ObjectPieces from them. Only on the peer's primary connection. */
        val recvStreams = HashMap<StreamKey, Stream>()
        val opening = HashSet<StreamKey>()
        val reopenAttempts = HashMap<StreamKey, Int>()
        var inboundStreams = 0
        var queuedMessages = 0
        var queuedBytes = 0L
    }

    private enum class SendResult { OK, FULL, NOTHING }

    // Everything below is confined to the event thread.
    private val state = WaggleState(config.maxCachedObjectsPerTopic, config.objectLifetime.toMillis(), clock)
    private val subscribedTopics = HashSet<WBytes>()
    private val connections = HashMap<Connection, ConnState>()
    private val peers = HashMap<PeerId, PeerState>()

    init {
        val sweepMillis = (config.objectLifetime.toMillis() / 4).coerceIn(1_000, 60_000)
        executor.scheduleWithFixedDelay({ guarded { state.sweep() } }, sweepMillis, sweepMillis, TimeUnit.MILLISECONDS)
    }

    fun addListener(listener: WaggleEventListener) {
        listeners += listener
    }

    fun removeListener(listener: WaggleEventListener) {
        listeners -= listener
    }

    // ---- Public API ---------------------------------------------------------------------------

    /** Opens a subscription stream for [topic] to every current and future peer. */
    fun subscribe(topic: ByteArray): CompletableFuture<Unit> = submit {
        val t = WBytes(topic.copyOf())
        if (subscribedTopics.add(t)) {
            peers.values.forEach { peer -> peer.primary?.also { openRecvStreams(it, t) } }
        }
    }

    /** Closes our subscription streams for [topic] and drops everything cached for it. */
    fun unsubscribe(topic: ByteArray): CompletableFuture<Unit> = submit {
        val t = WBytes(topic)
        if (subscribedTopics.remove(t)) {
            for (cs in connections.values) {
                val keys = cs.recvStreams.keys.filter { it.topic == t }
                keys.forEach { cs.recvStreams.remove(it)?.close() }
            }
            state.removeTopic(t)
        }
    }

    /**
     * Caches [shard] as the current snapshot of its object, then sends subscribed peers what they
     * lack: up to `forwardFanout` peers whose view of the object we already track, and up to
     * `publishFanout` randomly chosen peers we know nothing about yet. Up to `gossipFanout` of the
     * peers passed over get a metadata-only advert instead.
     *
     * Completes with the number of peers something was queued for (0 when nobody needed
     * anything). Fails with [NoSubscribersException] when no peer is subscribed (the snapshot
     * is still cached and offered to later subscribers) or [NotSentException] when every
     * peer that needed something had a full queue.
     */
    fun publish(topic: ByteArray, shard: WaggleShard): CompletableFuture<Int> = submit {
        val t = WBytes(topic)
        val objectId = WBytes(shard.objectId())
        state.store(t, shard)
        val subscribers = peers.values.filter { it.subscribedTo(t) }.shuffled(random)
        if (subscribers.isEmpty()) throw NoSubscribersException()

        val (known, unknown) = subscribers.partition { state.knows(t, it.peerId, objectId) }
        val topicConfig = topicConfig(t)
        var published = 0
        var full = 0
        var noData = 0
        val passedOver = ArrayList<PeerState>()
        for ((group, fanout) in listOf(known to topicConfig.forwardFanout, unknown to topicConfig.publishFanout)) {
            var sent = 0
            for (peer in group) {
                if (sent >= fanout) {
                    passedOver += peer
                    continue
                }
                when (reconcileAndSend(peer, t, objectId, withPieces = true)) {
                    SendResult.OK -> sent++
                    SendResult.FULL -> full++
                    SendResult.NOTHING -> noData++
                }
            }
            published += sent
        }
        // Peers beyond the payload fanouts still learn what we hold, so they can ask for it.
        var adverts = 0
        for (peer in passedOver) {
            if (adverts >= topicConfig.gossipFanout) break
            if (reconcileAndSend(peer, t, objectId, withPieces = false) == SendResult.OK) adverts++
        }
        if (published == 0 && full > 0) throw NotSentException(full, noData)
        published
    }

    /** Drops the cached snapshot of an object and what we tracked about peers for it. */
    fun removeObject(topic: ByteArray, objectId: ByteArray): CompletableFuture<Boolean> = submit {
        state.removeObject(WBytes(topic), WBytes(objectId))
    }

    /** Peers that currently hold a subscription to [topic] with us. */
    fun subscribers(topic: ByteArray): CompletableFuture<Set<PeerId>> = submit {
        val t = WBytes(topic)
        peers.values.filter { it.subscribedTo(t) }.map { it.peerId }.toSet()
    }

    // ---- libp2p wiring ------------------------------------------------------------------------

    override fun handleConnection(conn: Connection) {
        onEventThread { connState(conn) }
    }

    /** Responder side: the remote opened a stream to subscribe to one of our topics. */
    override fun initChannel(ch: P2PChannel, selectedProtocol: String): CompletableFuture<out Unit> {
        val stream = ch as Stream
        if (stream.isInitiator) {
            // Our own streams are created through SubscribeBinding, never through this binding.
            stream.close()
            return CompletableFuture.completedFuture(Unit)
        }
        stream.pushHandler(LimitedProtobufVarint32FrameDecoder(config.maxFrameSize))
        stream.pushHandler(ProtobufVarint32LengthFieldPrepender())
        stream.pushHandler(ProtobufDecoder(TopicSubscription.getDefaultInstance()))
        stream.pushHandler(ProtobufEncoder())
        stream.pushHandler(SendStreamHandler(stream))
        onEventThread {
            // Counted from stream open, so half-open streams cannot exceed the cap.
            val cs = connState(stream.connection) ?: return@onEventThread
            cs.inboundStreams++
            stream.closeFuture().thenRun { onEventThread { cs.inboundStreams-- } }
            if (cs.inboundStreams > config.maxInboundStreamsPerConnection) {
                logger.debug("Too many inbound Waggle streams from {}", cs.peer.peerId)
                stream.close()
            }
        }
        return CompletableFuture.completedFuture(Unit)
    }

    /** Initiator side: one binding instance per stream, because topic and kind are per-stream parameters. */
    private inner class SubscribeBinding(private val key: StreamKey) : ProtocolBinding<Stream> {
        override val protocolDescriptor = ProtocolDescriptor(WAGGLE_PROTOCOL_ID)

        override fun initChannel(ch: P2PChannel, selectedProtocol: String): CompletableFuture<out Stream> {
            val stream = ch as Stream
            stream.pushHandler(LimitedProtobufVarint32FrameDecoder(config.maxFrameSize))
            stream.pushHandler(ProtobufVarint32LengthFieldPrepender())
            stream.pushHandler(ProtobufDecoder(ObjectPieces.getDefaultInstance()))
            stream.pushHandler(ProtobufEncoder())
            stream.pushHandler(RecvStreamHandler(stream, key))
            return CompletableFuture.completedFuture(stream)
        }
    }

    /** Handler on a stream the remote opened. Reads the handshake, then the stream is write-only. */
    private inner class SendStreamHandler(private val stream: Stream) : ChannelDuplexHandler() {
        private var subscribed = false

        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            if (subscribed || msg !is TopicSubscription) {
                logger.debug("Unexpected message on a Waggle subscription stream from {}", stream.remotePeerId())
                stream.close()
                return
            }
            subscribed = true
            val key = StreamKey(WBytes(msg.topicID.toByteArray()), if (msg.hasStream()) msg.stream else CONTROL_STREAM)
            onEventThread { remoteSubscribed(stream, key) }
        }

        override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
            val size = (msg as? MessageLite)?.serializedSize ?: 0
            promise.addListener {
                onEventThread {
                    connections[stream.connection]?.also {
                        it.queuedMessages--
                        it.queuedBytes -= size
                    }
                }
            }
            ctx.write(msg, promise)
        }

        override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
            // The subscriber never writes after the handshake, so a write-close is an unsubscribe.
            if (evt is RemoteWriteClosed) stream.close()
            super.userEventTriggered(ctx, evt)
        }

        @Suppress("OVERRIDE_DEPRECATION")
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            logger.debug("Error on Waggle send stream", cause)
            stream.close()
        }
    }

    /** Handler on a stream we opened. Everything read is an ObjectPieces for the stream's topic. */
    private inner class RecvStreamHandler(private val stream: Stream, private val key: StreamKey) :
        ChannelInboundHandlerAdapter() {

        override fun handlerAdded(ctx: ChannelHandlerContext) {
            // The handshake cannot be written from initChannel: at that point the multistream
            // negotiation handlers are still in the pipeline and would add their own length
            // prefix. They are removed later in the same event loop task, so queue the write
            // behind it.
            ctx.executor().execute {
                val handshake = TopicSubscription.newBuilder().setTopicID(ByteString.copyFrom(key.topic.array))
                // The control stream omits the field, which is what peers without the extension send.
                if (key.kind != CONTROL_STREAM) handshake.stream = key.kind
                ctx.writeAndFlush(handshake.build())
            }
        }

        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            msg as ObjectPieces
            onEventThread { handleReceived(stream.connection, key, msg) }
        }

        @Suppress("OVERRIDE_DEPRECATION")
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            logger.debug("Error on Waggle receive stream", cause)
            stream.close()
        }
    }

    // ---- Event thread: connections and streams ------------------------------------------------

    /** The state of [conn], created on first use. Null if the connection is already closed. */
    private fun connState(conn: Connection): ConnState? {
        connections[conn]?.also { return it }
        if (conn.closeFuture().isDone) return null
        val peer = peers.getOrPut(conn.secureSession().remoteId) { PeerState(conn.secureSession().remoteId) }
        val cs = ConnState(conn, peer)
        connections[conn] = cs
        peer.connections += cs
        conn.closeFuture().thenRun { onEventThread { connectionClosed(cs) } }
        if (peer.primary == null) makePrimary(cs)
        return cs
    }

    private fun makePrimary(cs: ConnState) {
        cs.peer.primary = cs
        subscribedTopics.forEach { openRecvStreams(cs, it) }
    }

    private fun connectionClosed(cs: ConnState) {
        val peer = cs.peer
        connections.remove(cs.connection)
        peer.connections -= cs
        if (peer.connections.isEmpty()) {
            peers.remove(peer.peerId)
            state.peerGone(peer.peerId)
        } else if (peer.primary === cs) {
            // Move our subscriptions to a connection that is still alive.
            makePrimary(peer.connections.first())
        }
    }

    private fun streamKinds(topic: WBytes) =
        if (topicConfig(topic).dataStream) listOf(CONTROL_STREAM, DATA_STREAM) else listOf(CONTROL_STREAM)

    private fun openRecvStreams(cs: ConnState, topic: WBytes) {
        streamKinds(topic).forEach { openRecvStream(cs, StreamKey(topic, it)) }
    }

    /** True while [cs] is where our subscription to [topic] belongs. */
    private fun wantsRecvStream(cs: ConnState, topic: WBytes) =
        topic in subscribedTopics && connections[cs.connection] === cs && cs.peer.primary === cs

    private fun openRecvStream(cs: ConnState, key: StreamKey) {
        if (key in cs.recvStreams || !cs.opening.add(key)) return
        cs.connection.muxerSession().createStream(SubscribeBinding(key)).controller
            .whenComplete { stream, error ->
                onEventThread {
                    cs.opening -= key
                    when {
                        error != null -> {
                            // Typically the peer does not speak Waggle. Reported, never retried.
                            if (connections[cs.connection] === cs) notifyStreamFailed(cs, key, error, false)
                        }
                        !wantsRecvStream(cs, key.topic) || key in cs.recvStreams -> stream.close()
                        else -> {
                            cs.recvStreams[key] = stream
                            stream.closeFuture().thenRun { onEventThread { recvStreamClosed(cs, key, stream) } }
                        }
                    }
                }
            }
    }

    /**
     * The remote closed a stream we still want. Reopen it after a backoff that doubles with every
     * consecutive attempt; receiving anything on the stream resets the count. A remote that keeps
     * rejecting the subscription is given up on after `maxReopenAttempts`.
     */
    private fun recvStreamClosed(cs: ConnState, key: StreamKey, stream: Stream) {
        if (cs.recvStreams[key] !== stream) return // we closed it ourselves
        cs.recvStreams.remove(key)
        if (!wantsRecvStream(cs, key.topic)) return

        val attempts = cs.reopenAttempts[key] ?: 0
        val willRetry = attempts < config.maxReopenAttempts
        notifyStreamFailed(cs, key, null, willRetry)
        if (!willRetry) return
        cs.reopenAttempts[key] = attempts + 1
        val delay = (config.reopenInitialBackoff.toMillis() shl attempts.coerceAtMost(20))
            .coerceAtMost(config.reopenMaxBackoff.toMillis())
        executor.schedule(
            { guarded { if (wantsRecvStream(cs, key.topic)) openRecvStream(cs, key) } },
            delay,
            TimeUnit.MILLISECONDS
        )
    }

    private fun notifyStreamFailed(cs: ConnState, key: StreamKey, cause: Throwable?, willRetry: Boolean) {
        // A peer without the data stream extension rejects the second stream; that is expected.
        if (key.kind != CONTROL_STREAM) {
            logger.debug("Waggle data stream to {} failed (retry: {})", cs.peer.peerId, willRetry, cause)
            return
        }
        listeners.forEach { it.onStreamFailed(cs.peer.peerId, cs.connection, key.topic.array, cause, willRetry) }
    }

    private fun remoteSubscribed(stream: Stream, key: StreamKey) {
        val cs = connState(stream.connection)
        if (cs == null ||
            key.topic.array.size > config.maxTopicIdSize ||
            key.kind !in CONTROL_STREAM..DATA_STREAM ||
            key in cs.sendStreams ||
            !config.topicFilter(key.topic.array)
        ) {
            logger.debug("Rejecting Waggle subscription from {}", stream.remotePeerId())
            stream.close()
            return
        }
        val peer = cs.peer
        cs.sendStreams[key] = stream
        stream.closeFuture().thenRun {
            onEventThread {
                if (cs.sendStreams[key] === stream) {
                    cs.sendStreams.remove(key)
                    if (key.kind == CONTROL_STREAM) {
                        if (!peer.subscribedTo(key.topic)) state.peerUnsubscribed(key.topic, peer.peerId)
                        listeners.forEach { it.onUnsubscribed(peer.peerId, cs.connection, key.topic.array) }
                    }
                }
            }
        }
        if (key.kind != CONTROL_STREAM) return
        listeners.forEach { it.onSubscribed(peer.peerId, cs.connection, key.topic.array) }
        // Seed the new subscriber with what we already hold for the topic.
        state.cachedObjects(key.topic).forEach { reconcileAndSend(peer, key.topic, it, withPieces = true) }
    }

    // ---- Event thread: dissemination ----------------------------------------------------------

    private fun handleReceived(conn: Connection, key: StreamKey, msg: ObjectPieces) {
        val cs = connections[conn] ?: return
        val topic = key.topic
        if (topic !in subscribedTopics) return
        cs.reopenAttempts.remove(key)
        val peer = cs.peer
        val objectId = WBytes(msg.objectID.toByteArray())
        val metadata = if (msg.hasPiecesMetadata()) msg.piecesMetadata.toByteArray() else null
        val pieces = if (msg.hasPieces()) msg.pieces.toByteArray() else null

        val outcome = try {
            state.received(topic, peer.peerId, objectId, metadata, pieces)
        } catch (e: Exception) {
            listeners.forEach { it.onInvalidData(peer.peerId, conn, topic.array, objectId.array, e) }
            return
        } ?: return

        if (outcome.emit) {
            listeners.forEach { it.onReceived(peer.peerId, conn, topic.array, objectId.array, metadata, pieces) }
        }

        outcome.reply?.also { reply ->
            when (trySend(peer, topic, reply)) {
                SendResult.OK -> state.commit(reply)
                SendResult.FULL -> listeners.forEach { it.onFullQueue(peer.peerId, conn) }
                SendResult.NOTHING -> {}
            }
        }

        // Tell other subscribers what we hold, if they have not seen our current metadata.
        val fanout = topicConfig(topic).gossipFanout
        var sent = 0
        for (other in peers.values.filter { it !== peer && it.subscribedTo(topic) }.shuffled(random)) {
            if (sent >= fanout) break
            if (reconcileAndSend(other, topic, objectId, withPieces = false) == SendResult.OK) sent++
        }
    }

    /** @return OK if something was queued, FULL if it was dropped, NOTHING if there was nothing to send. */
    private fun reconcileAndSend(peer: PeerState, topic: WBytes, objectId: WBytes, withPieces: Boolean): SendResult {
        val transmission = try {
            state.prepare(topic, peer.peerId, objectId, withPieces)
        } catch (e: Exception) {
            val conn = peer.sendStream(StreamKey(topic, CONTROL_STREAM))?.first?.connection
            if (conn != null) {
                listeners.forEach { it.onInvalidData(peer.peerId, conn, topic.array, objectId.array, e) }
            }
            null
        } ?: return SendResult.NOTHING
        return trySend(peer, topic, transmission).also {
            if (it == SendResult.OK) state.commit(transmission)
        }
    }

    private fun trySend(peer: PeerState, topic: WBytes, t: WaggleState.Transmission): SendResult {
        // Payload goes to the data stream when the remote opened one, everything else to control.
        val (cs, stream) = (if (t.pieces != null) peer.sendStream(StreamKey(topic, DATA_STREAM)) else null)
            ?: peer.sendStream(StreamKey(topic, CONTROL_STREAM))
            ?: return SendResult.NOTHING
        val builder = ObjectPieces.newBuilder().setObjectID(ByteString.copyFrom(t.objectId))
        t.metadata?.also { builder.piecesMetadata = ByteString.copyFrom(it) }
        t.pieces?.also { builder.pieces = ByteString.copyFrom(it) }
        val msg = builder.build()
        val size = msg.serializedSize
        if (cs.queuedMessages >= config.maxQueuedMessagesPerConnection ||
            cs.queuedBytes + size > config.maxQueuedBytesPerConnection
        ) {
            return SendResult.FULL
        }
        cs.queuedMessages++
        cs.queuedBytes += size
        stream.writeAndFlush(msg)
        return SendResult.OK
    }

    private fun topicConfig(topic: WBytes): WaggleTopicConfig =
        config.topicConfigs[String(topic.array, Charsets.UTF_8)] ?: config.defaultTopicConfig

    private fun <T> submit(block: () -> T): CompletableFuture<T> = CompletableFuture.supplyAsync(block, executor)

    /** Runs [block] on the event thread. A failure is logged, never silently dropped. */
    private fun onEventThread(block: () -> Unit) {
        executor.execute { guarded(block) }
    }

    private fun guarded(block: () -> Unit) {
        try {
            block()
        } catch (e: Throwable) {
            logger.error("Unexpected error on the Waggle event thread", e)
        }
    }
}
