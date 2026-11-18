package io.libp2p.pubsub.waggle

import com.google.protobuf.ByteString
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

const val WAGGLE_PROTOCOL_ID = "/waggle/1.0"

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
 * All state lives on a single event thread; listeners are called on it.
 */
class Waggle @JvmOverloads constructor(
    private val config: WaggleConfig = WaggleConfig(),
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "waggle-event-thread").also { it.isDaemon = true }
    },
    private val random: Random = Random()
) : ProtocolBinding<Unit>, ConnectionHandler {

    private val logger = LoggerFactory.getLogger(Waggle::class.java)

    override val protocolDescriptor = ProtocolDescriptor(WAGGLE_PROTOCOL_ID)

    private val listeners = CopyOnWriteArrayList<WaggleEventListener>()

    private class ConnState(val connection: Connection) {
        val peerId: PeerId = connection.secureSession().remoteId

        /** Streams the remote opened: we write the topic's ObjectPieces to them. */
        val sendStreams = HashMap<WBytes, Stream>()

        /** Streams we opened: we read the topic's ObjectPieces from them. */
        val recvStreams = HashMap<WBytes, Stream>()
        var inboundStreams = 0
        var queuedMessages = 0
    }

    private enum class SendResult { OK, FULL, CLOSED }

    // Everything below is confined to the event thread.
    private val state = WaggleState(config.maxCachedObjectsPerTopic)
    private val subscribedTopics = HashSet<WBytes>()
    private val connections = HashMap<Connection, ConnState>()

    fun addListener(listener: WaggleEventListener) {
        listeners += listener
    }

    fun removeListener(listener: WaggleEventListener) {
        listeners -= listener
    }

    // ---- Public API ---------------------------------------------------------------------------

    /** Opens a subscription stream for [topic] on every current and future connection. */
    fun subscribe(topic: ByteArray): CompletableFuture<Unit> = submit {
        val t = WBytes(topic.copyOf())
        if (subscribedTopics.add(t)) {
            connections.values.forEach { openRecvStream(it, t) }
        }
    }

    /** Closes our subscription streams for [topic] and drops everything cached for it. */
    fun unsubscribe(topic: ByteArray): CompletableFuture<Unit> = submit {
        val t = WBytes(topic)
        if (subscribedTopics.remove(t)) {
            connections.values.forEach { it.recvStreams.remove(t)?.close() }
            state.removeTopic(t)
        }
    }

    /**
     * Caches [shard] as the current snapshot of its object and sends each of up to `publishFanout`
     * randomly chosen subscribed connections the pieces it lacks.
     *
     * Completes with the number of connections something was queued for (0 when nobody needed
     * anything). Fails with [NoSubscribersException] when no connection is subscribed (the snapshot
     * is still cached and offered to later subscribers) or [NotSentException] when every
     * connection that needed something had a full queue.
     */
    fun publish(topic: ByteArray, shard: WaggleShard): CompletableFuture<Int> = submit {
        val t = WBytes(topic)
        val objectId = WBytes(shard.objectId())
        state.store(t, shard)
        val eligible = connections.values.filter { t in it.sendStreams }.shuffled(random)
        if (eligible.isEmpty()) throw NoSubscribersException()

        val fanout = topicConfig(t).publishFanout
        var published = 0
        var full = 0
        var noData = 0
        for (cs in eligible) {
            if (published >= fanout) break
            when (reconcileAndSend(cs, t, objectId, withPieces = true)) {
                SendResult.OK -> published++
                SendResult.FULL -> full++
                else -> noData++
            }
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
        connections.values.filter { t in it.sendStreams }.map { it.peerId }.toSet()
    }

    // ---- libp2p wiring ------------------------------------------------------------------------

    override fun handleConnection(conn: Connection) {
        onEventThread {
            val cs = connState(conn)
            subscribedTopics.forEach { openRecvStream(cs, it) }
        }
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
            val cs = connState(stream.connection)
            cs.inboundStreams++
            stream.closeFuture().thenRun { onEventThread { cs.inboundStreams-- } }
            if (cs.inboundStreams > config.maxInboundStreamsPerConnection) {
                logger.debug("Too many inbound Waggle streams from {}", cs.peerId)
                stream.close()
            }
        }
        return CompletableFuture.completedFuture(Unit)
    }

    /** Initiator side: one binding instance per stream, because the topic is a per-stream parameter. */
    private inner class SubscribeBinding(private val topic: WBytes) : ProtocolBinding<Stream> {
        override val protocolDescriptor = ProtocolDescriptor(WAGGLE_PROTOCOL_ID)

        override fun initChannel(ch: P2PChannel, selectedProtocol: String): CompletableFuture<out Stream> {
            val stream = ch as Stream
            stream.pushHandler(LimitedProtobufVarint32FrameDecoder(config.maxFrameSize))
            stream.pushHandler(ProtobufVarint32LengthFieldPrepender())
            stream.pushHandler(ProtobufDecoder(ObjectPieces.getDefaultInstance()))
            stream.pushHandler(ProtobufEncoder())
            stream.pushHandler(RecvStreamHandler(stream, topic))
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
            val topic = WBytes(msg.topicID.toByteArray())
            onEventThread { remoteSubscribed(stream, topic) }
        }

        override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
            promise.addListener {
                onEventThread { connections[stream.connection]?.also { it.queuedMessages-- } }
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

    /** Handler on a stream we opened. Everything read is an ObjectPieces for [topic]. */
    private inner class RecvStreamHandler(private val stream: Stream, private val topic: WBytes) :
        ChannelInboundHandlerAdapter() {

        override fun handlerAdded(ctx: ChannelHandlerContext) {
            // The handshake cannot be written from initChannel: at that point the multistream
            // negotiation handlers are still in the pipeline and would add their own length
            // prefix. They are removed later in the same event loop task, so queue the write
            // behind it.
            ctx.executor().execute {
                ctx.writeAndFlush(TopicSubscription.newBuilder().setTopicID(ByteString.copyFrom(topic.array)).build())
            }
        }

        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            msg as ObjectPieces
            onEventThread { handleReceived(stream.connection, topic, msg) }
        }

        @Suppress("OVERRIDE_DEPRECATION")
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            logger.debug("Error on Waggle receive stream", cause)
            stream.close()
        }
    }

    // ---- Event thread -------------------------------------------------------------------------

    private fun connState(conn: Connection): ConnState =
        connections.getOrPut(conn) {
            conn.closeFuture().thenRunAsync(
                {
                    connections.remove(conn)
                    state.connectionClosed(conn)
                },
                executor
            )
            ConnState(conn)
        }

    private fun openRecvStream(cs: ConnState, topic: WBytes) {
        if (topic in cs.recvStreams) return
        cs.connection.muxerSession().createStream(SubscribeBinding(topic)).controller
            .whenCompleteAsync(
                { stream, error ->
                    when {
                        error != null -> {
                            // Typically the peer does not speak Waggle. Reported, never retried.
                            listeners.forEach { it.onStreamFailed(cs.peerId, cs.connection, topic.array, error) }
                        }
                        topic !in subscribedTopics || topic in cs.recvStreams -> stream.close()
                        else -> {
                            cs.recvStreams[topic] = stream
                            stream.closeFuture().thenRunAsync({ recvStreamClosed(cs, topic, stream) }, executor)
                        }
                    }
                },
                executor
            )
    }

    private fun recvStreamClosed(cs: ConnState, topic: WBytes, stream: Stream) {
        if (cs.recvStreams[topic] !== stream) return
        cs.recvStreams.remove(topic)
        // Still subscribed and the connection is alive: the remote dropped our subscription.
        if (topic in subscribedTopics && cs.connection in connections) {
            listeners.forEach { it.onStreamFailed(cs.peerId, cs.connection, topic.array, null) }
        }
    }

    private fun remoteSubscribed(stream: Stream, topic: WBytes) {
        val cs = connState(stream.connection)
        if (topic.array.size > config.maxTopicIdSize || topic in cs.sendStreams) {
            logger.debug("Rejecting Waggle subscription from {} (duplicate or oversized topic)", cs.peerId)
            stream.close()
            return
        }
        cs.sendStreams[topic] = stream
        stream.closeFuture().thenRunAsync(
            {
                if (cs.sendStreams[topic] === stream) {
                    cs.sendStreams.remove(topic)
                    state.connectionUnsubscribed(topic, cs.connection)
                    listeners.forEach { it.onUnsubscribed(cs.peerId, cs.connection, topic.array) }
                }
            },
            executor
        )
        listeners.forEach { it.onSubscribed(cs.peerId, cs.connection, topic.array) }
        // Seed the new subscriber with what we already hold for the topic.
        state.cachedObjects(topic).forEach { reconcileAndSend(cs, topic, it, withPieces = true) }
    }

    private fun handleReceived(conn: Connection, topic: WBytes, msg: ObjectPieces) {
        val cs = connections[conn] ?: return
        if (topic !in subscribedTopics) return
        val objectId = WBytes(msg.objectID.toByteArray())
        val metadata = if (msg.hasPiecesMetadata()) msg.piecesMetadata.toByteArray() else null
        val pieces = if (msg.hasPieces()) msg.pieces.toByteArray() else null

        val outcome = try {
            state.received(topic, conn, cs.peerId, objectId, metadata, pieces)
        } catch (e: Exception) {
            listeners.forEach { it.onInvalidData(cs.peerId, conn, topic.array, objectId.array, e) }
            return
        } ?: return

        if (outcome.emit) {
            listeners.forEach { it.onReceived(cs.peerId, conn, topic.array, objectId.array, metadata, pieces) }
        }

        outcome.reply?.also { reply ->
            when (trySend(cs, topic, reply)) {
                SendResult.OK -> state.commit(reply)
                SendResult.FULL -> listeners.forEach { it.onFullQueue(cs.peerId, conn) }
                SendResult.CLOSED -> {}
            }
        }

        // Tell other subscribers what we hold, if they have not seen our current metadata.
        val fanout = topicConfig(topic).gossipFanout
        var sent = 0
        for (other in connections.values.filter { it !== cs && topic in it.sendStreams }.shuffled(random)) {
            if (sent >= fanout) break
            if (reconcileAndSend(other, topic, objectId, withPieces = false) == SendResult.OK) sent++
        }
    }

    /** @return OK if something was queued, FULL if it was dropped, CLOSED if there was nothing to send. */
    private fun reconcileAndSend(cs: ConnState, topic: WBytes, objectId: WBytes, withPieces: Boolean): SendResult {
        val transmission = try {
            state.prepare(topic, cs.connection, cs.peerId, objectId, withPieces)
        } catch (e: Exception) {
            listeners.forEach { it.onInvalidData(cs.peerId, cs.connection, topic.array, objectId.array, e) }
            null
        } ?: return SendResult.CLOSED
        return trySend(cs, topic, transmission).also {
            if (it == SendResult.OK) state.commit(transmission)
        }
    }

    private fun trySend(cs: ConnState, topic: WBytes, t: WaggleState.Transmission): SendResult {
        val stream = cs.sendStreams[topic] ?: return SendResult.CLOSED
        if (cs.queuedMessages >= config.maxQueuedMessagesPerConnection) return SendResult.FULL
        val msg = ObjectPieces.newBuilder().setObjectID(ByteString.copyFrom(t.objectId))
        t.metadata?.also { msg.piecesMetadata = ByteString.copyFrom(it) }
        t.pieces?.also { msg.pieces = ByteString.copyFrom(it) }
        cs.queuedMessages++
        stream.writeAndFlush(msg.build())
        return SendResult.OK
    }

    private fun topicConfig(topic: WBytes): WaggleTopicConfig =
        config.topicConfigs[String(topic.array, Charsets.UTF_8)] ?: config.defaultTopicConfig

    private fun <T> submit(block: () -> T): CompletableFuture<T> = CompletableFuture.supplyAsync(block, executor)

    /** Runs [block] on the event thread. A failure is logged, never silently dropped. */
    private fun onEventThread(block: () -> Unit) {
        executor.execute {
            try {
                block()
            } catch (e: Throwable) {
                logger.error("Unexpected error on the Waggle event thread", e)
            }
        }
    }
}
