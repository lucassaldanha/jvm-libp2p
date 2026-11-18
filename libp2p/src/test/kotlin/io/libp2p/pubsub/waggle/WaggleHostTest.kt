package io.libp2p.pubsub.waggle

import io.libp2p.core.Connection
import io.libp2p.core.PeerId
import io.libp2p.core.multistream.ProtocolBinding
import io.libp2p.protocol.Identify
import io.libp2p.tools.HostFactory
import io.libp2p.tools.TestHost
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

class WaggleHostTest {
    private val topic = "test-topic".toByteArray()
    private val hostFactory = HostFactory().also { it.muxLogLevel = null }

    /** A node whose application integrates every piece it receives and republishes, like a real client. */
    private inner class Node(config: WaggleConfig = WaggleConfig()) {
        val waggle = Waggle(config)
        val objects = ConcurrentHashMap<Byte, TestShard>()
        val subscribedPeers = CopyOnWriteArrayList<PeerId>()
        val unsubscribedPeers = CopyOnWriteArrayList<PeerId>()
        val receivedFrom = CopyOnWriteArrayList<PeerId>()

        val invalidData = CopyOnWriteArrayList<Throwable>()

        /** (piecesSize, dataStream) of every frame sent and received. */
        val framesSent = CopyOnWriteArrayList<Pair<Int, Boolean>>()
        val framesReceived = CopyOnWriteArrayList<Pair<Int, Boolean>>()

        /** willRetry of every onStreamFailed, in order. */
        val streamFailures = CopyOnWriteArrayList<Boolean>()
        val host: TestHost

        init {
            waggle.addListener(object : WaggleEventListener {
                override fun onReceived(
                    peer: PeerId,
                    connection: Connection,
                    topic: ByteArray,
                    objectId: ByteArray,
                    metadata: ByteArray?,
                    pieces: ByteArray?
                ) {
                    receivedFrom += peer
                    val incoming = if (pieces != null) pieces[0].toInt() and 0xFF else 0
                    waggle.publish(topic, integrate(objectId[0], incoming))
                }

                override fun onSubscribed(peer: PeerId, connection: Connection, topic: ByteArray) {
                    subscribedPeers += peer
                }

                override fun onUnsubscribed(peer: PeerId, connection: Connection, topic: ByteArray) {
                    unsubscribedPeers += peer
                }

                override fun onInvalidData(peer: PeerId, connection: Connection, topic: ByteArray, objectId: ByteArray, cause: Throwable) {
                    invalidData += cause
                }

                override fun onFrameSent(
                    peer: PeerId,
                    connection: Connection,
                    topic: ByteArray,
                    objectId: ByteArray,
                    metadataSize: Int,
                    piecesSize: Int,
                    dataStream: Boolean
                ) {
                    framesSent += piecesSize to dataStream
                }

                override fun onFrameReceived(
                    peer: PeerId,
                    connection: Connection,
                    topic: ByteArray,
                    objectId: ByteArray,
                    metadataSize: Int,
                    piecesSize: Int,
                    dataStream: Boolean
                ) {
                    framesReceived += piecesSize to dataStream
                }

                override fun onStreamFailed(peer: PeerId, connection: Connection, topic: ByteArray, cause: Throwable?, willRetry: Boolean) {
                    streamFailures += willRetry
                }
            })
            hostFactory.protocols = listOf<ProtocolBinding<Any>>(Identify(), waggle)
            host = hostFactory.createHost()
        }

        fun connect(other: Node): Connection =
            host.host.network.connect(other.host.peerId, other.host.listenAddress).get(5, TimeUnit.SECONDS)

        /** Network.connect reuses an existing connection, so a second one is dialed via the transport. */
        fun dialAdditionalConnection(other: Node): Connection {
            val network = host.host.network
            val addr = other.host.listenAddress.withP2P(other.host.peerId)
            val transport = network.transports.first { it.handles(addr) }
            val conn = transport.dial(addr, network.connectionHandler).get(5, TimeUnit.SECONDS)
            waitFor("second connection") {
                other.host.host.network.connections.count { it.secureSession().remoteId == host.peerId } == 2
            }
            return conn
        }

        /**
         * Adds pieces to what we hold, atomically: the test thread and the event thread both do
         * this, and a snapshot must never lose pieces a peer already believes we have.
         */
        fun integrate(id: Byte, bits: Int): TestShard =
            objects.merge(id, TestShard(id, bits)) { held, added -> TestShard(id, held.bits or added.bits) }!!

        fun publish(shard: TestShard) {
            waggle.publish(topic, integrate(shard.id, shard.bits))
        }

        fun bits(id: Byte) = objects[id]?.bits ?: 0
    }

    @AfterEach
    fun cleanup() {
        hostFactory.shutdown()
    }

    private fun waitFor(what: String, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 10_000
        while (!condition()) {
            check(System.currentTimeMillis() < deadline) { "Timed out waiting for: $what" }
            Thread.sleep(20)
        }
    }

    private fun subscribeBoth(a: Node, b: Node) {
        a.waggle.subscribe(topic).get()
        b.waggle.subscribe(topic).get()
        waitFor("mutual subscription") { a.subscribedPeers.size == 1 && b.subscribedPeers.size == 1 }
    }

    @Test
    fun `two nodes with complementary pieces both reconstruct the object`() {
        val a = Node()
        val b = Node()
        a.connect(b)
        subscribeBoth(a, b)

        a.publish(TestShard(1, 0b00001111))
        b.publish(TestShard(1, 0b11110000))

        waitFor("both complete") { a.bits(1) == 0xFF && b.bits(1) == 0xFF }
    }

    @Test
    fun `pieces relay across a line of three nodes`() {
        val a = Node()
        val b = Node()
        val c = Node()
        a.connect(b)
        b.connect(c)
        listOf(a, b, c).forEach { it.waggle.subscribe(topic).get() }
        waitFor("subscriptions") { a.subscribedPeers.size == 1 && b.subscribedPeers.size == 2 && c.subscribedPeers.size == 1 }

        a.publish(TestShard(7, 0b00000111))
        c.publish(TestShard(7, 0b11111000))

        waitFor("all complete") { listOf(a, b, c).all { it.bits(7) == 0xFF } }
    }

    @Test
    fun `a node that only hears an advert pulls the pieces`() {
        // Hub b never publishes payload to c (publishFanout and forwardFanout are 0): c only gets
        // b's metadata advert and has to ask.
        val a = Node()
        val b = Node(WaggleConfig(defaultTopicConfig = WaggleTopicConfig(publishFanout = 0, forwardFanout = 0)))
        val c = Node()
        a.connect(b)
        b.connect(c)
        listOf(a, b, c).forEach { it.waggle.subscribe(topic).get() }
        waitFor("subscriptions") { a.subscribedPeers.size == 1 && b.subscribedPeers.size == 2 && c.subscribedPeers.size == 1 }

        a.publish(TestShard(5, 0xFF))

        waitFor("c complete") { c.bits(5) == 0xFF }
    }

    @Test
    fun `a late subscriber is seeded with cached objects`() {
        val a = Node()
        val b = Node()
        a.waggle.subscribe(topic).get()
        a.publish(TestShard(3, 0xFF)) // nobody is subscribed yet: cached only
        a.connect(b)
        b.waggle.subscribe(topic).get()

        waitFor("late subscriber complete") { b.bits(3) == 0xFF }
    }

    @Test
    fun `unsubscribing closes the stream and the remote stops sending`() {
        val a = Node()
        val b = Node()
        a.connect(b)
        subscribeBoth(a, b)

        b.waggle.unsubscribe(topic).get()
        waitFor("a sees b unsubscribe") { a.unsubscribedPeers.size == 1 }
        assertThat(a.waggle.subscribers(topic).get()).isEmpty()
        // b still serves a: a's own subscription stream is untouched.
        assertThat(b.waggle.subscribers(topic).get()).containsExactly(a.host.peerId)
        // An unsubscribe is our own doing, not a stream failure.
        assertThat(b.streamFailures).isEmpty()
    }

    @Test
    fun `a subscription the remote closed is reopened after a backoff`() {
        var accept = false
        val a = Node(WaggleConfig(reopenInitialBackoff = Duration.ofMillis(50)))
        val b = Node(WaggleConfig(topicFilter = { accept }))
        a.connect(b)
        a.waggle.subscribe(topic).get()

        waitFor("first rejection") { a.streamFailures.isNotEmpty() }
        assertThat(a.streamFailures.first()).isTrue()
        assertThat(b.subscribedPeers).isEmpty()

        accept = true
        waitFor("subscription after retry") { b.subscribedPeers.size == 1 }
        b.publish(TestShard(4, 0xFF))
        waitFor("delivery on the reopened stream") { a.bits(4) == 0xFF }
    }

    @Test
    fun `reopening gives up after the configured attempts`() {
        val a = Node(WaggleConfig(reopenInitialBackoff = Duration.ofMillis(10), maxReopenAttempts = 2))
        val b = Node(WaggleConfig(topicFilter = { false }))
        a.connect(b)
        a.waggle.subscribe(topic).get()

        waitFor("gave up") { a.streamFailures.lastOrNull() == false }
        // The first open plus two retries, the last one reported as final.
        assertThat(a.streamFailures).containsExactly(true, true, false)
        Thread.sleep(200)
        assertThat(a.streamFailures).hasSize(3)
    }

    @Test
    fun `two connections to a peer carry one subscription which survives closing its connection`() {
        val a = Node()
        val b = Node()
        val first = a.connect(b)
        val second = a.dialAdditionalConnection(b)
        subscribeBoth(a, b)
        Thread.sleep(300)
        // One subscription each way, despite two connections.
        assertThat(a.subscribedPeers).hasSize(1)
        assertThat(b.subscribedPeers).hasSize(1)

        a.publish(TestShard(1, 0b00001111))
        b.publish(TestShard(1, 0b11110000))
        waitFor("both complete") { a.bits(1) == 0xFF && b.bits(1) == 0xFF }

        // Close the connection the subscriptions are on: they move to the other one.
        first.close().get(5, TimeUnit.SECONDS)
        waitFor("resubscribed on the remaining connection") { a.subscribedPeers.size == 2 && b.subscribedPeers.size == 2 }
        assertThat(second.closeFuture().isDone).isFalse()

        b.publish(TestShard(2, 0xFF))
        waitFor("delivery after failover") { a.bits(2) == 0xFF }
    }

    @Test
    fun `a data stream carries the payload when one side asks for it`() {
        val a = Node(WaggleConfig(defaultTopicConfig = WaggleTopicConfig(dataStream = true)))
        val b = Node()
        a.connect(b)
        subscribeBoth(a, b)
        // The data stream is not an extra subscription.
        Thread.sleep(300)
        assertThat(b.subscribedPeers).hasSize(1)

        a.publish(TestShard(1, 0b00001111))
        b.publish(TestShard(1, 0b11110000))
        waitFor("both complete") { a.bits(1) == 0xFF && b.bits(1) == 0xFF }
        assertThat(a.streamFailures).isEmpty()

        // a asked for a data stream: everything b sent with pieces went on it, the rest did not.
        assertThat(b.framesSent.filter { it.first >= 0 }).isNotEmpty.allMatch { it.second }
        assertThat(b.framesSent.filter { it.first < 0 }).noneMatch { it.second }
        assertThat(a.framesReceived.filter { it.first >= 0 }).isNotEmpty.allMatch { it.second }
        // b did not ask for one: a sends it everything on the single stream.
        assertThat(a.framesSent).isNotEmpty.noneMatch { it.second }
    }

    @Test
    fun `an inbound stream that never sends its handshake is closed`() {
        val a = Node()
        val b = Node(WaggleConfig(handshakeTimeout = Duration.ofMillis(200)))
        a.connect(b)
        // Open a /waggle/1.0 stream to b and say nothing.
        val silent = ProtocolBinding.createSimple(WAGGLE_PROTOCOL_ID) { ch -> CompletableFuture.completedFuture(ch) }
        val stream = silent.dial(a.host.host, b.host.peerId, b.host.listenAddress).stream.get(5, TimeUnit.SECONDS)

        waitFor("silent stream closed") { stream.closeFuture().isDone }
        assertThat(b.subscribedPeers).isEmpty()
    }

    @Test
    fun `a frame larger than the limit is reported and not sent`() {
        val a = Node(WaggleConfig(maxFrameSize = 16))
        val b = Node()
        a.connect(b)
        subscribeBoth(a, b)

        a.publish(TestShard(1, 0xFF)) // 9 bytes of pieces plus id and metadata: 17 bytes

        waitFor("reported") { a.invalidData.isNotEmpty() }
        assertThat(a.invalidData.first()).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(a.framesSent).isEmpty()
        assertThat(b.bits(1)).isEqualTo(0)
    }

    @Test
    fun `a raw topic hands frames straight to the application and sends on request`() {
        class RawNode {
            val waggle = Waggle()
            val messages = CopyOnWriteArrayList<Triple<PeerId, ByteArray?, ByteArray?>>()
            val host: TestHost

            init {
                hostFactory.protocols = listOf<ProtocolBinding<Any>>(Identify(), waggle)
                host = hostFactory.createHost()
            }
        }
        val a = RawNode()
        val b = RawNode()
        a.host.host.network.connect(b.host.peerId, b.host.listenAddress).get(5, TimeUnit.SECONDS)
        a.waggle.subscribeRaw(topic) { peer, _, _, metadata, pieces -> a.messages += Triple(peer, metadata, pieces) }.get()
        assertThat(a.waggle.sendCapacity(topic, b.host.peerId).get()).isEqualTo(0)
        b.waggle.subscribeRaw(topic) { peer, _, _, metadata, pieces -> b.messages += Triple(peer, metadata, pieces) }.get()
        waitFor("mutual subscription") {
            a.waggle.subscribers(topic).get().isNotEmpty() && b.waggle.subscribers(topic).get().isNotEmpty()
        }
        assertThat(a.waggle.sendCapacity(topic, b.host.peerId).get()).isEqualTo(WaggleConfig().maxQueuedMessagesPerConnection)

        // The future completes once the frame is written; absent and empty fields stay distinct.
        a.waggle.send(topic, b.host.peerId, byteArrayOf(1), byteArrayOf(), null).get(5, TimeUnit.SECONDS)
        a.waggle.send(topic, b.host.peerId, byteArrayOf(1), null, byteArrayOf(7, 8)).get(5, TimeUnit.SECONDS)
        waitFor("b got both frames") { b.messages.size == 2 }
        assertThat(b.messages[0].first).isEqualTo(a.host.peerId)
        assertThat(b.messages[0].second).isEmpty()
        assertThat(b.messages[0].third).isNull()
        assertThat(b.messages[1].second).isNull()
        assertThat(b.messages[1].third).containsExactly(7, 8)
        // Nothing is answered or gossiped on a raw topic's behalf.
        Thread.sleep(200)
        assertThat(a.messages).isEmpty()

        val toStranger = runCatching { a.waggle.send(topic, PeerId.random(), byteArrayOf(1), null, null).get(5, TimeUnit.SECONDS) }
        assertThat(toStranger.exceptionOrNull()?.cause).isInstanceOf(PeerNotSubscribedException::class.java)
        val managed = runCatching { a.waggle.publish(topic, TestShard(1, 1)).get(5, TimeUnit.SECONDS) }
        assertThat(managed.exceptionOrNull()?.cause).isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `a raw node pulls an object from a managed node`() {
        val managed = Node()
        val rawWaggle = Waggle()
        val got = CopyOnWriteArrayList<ByteArray>()
        hostFactory.protocols = listOf<ProtocolBinding<Any>>(Identify(), rawWaggle)
        val rawHost = hostFactory.createHost()
        rawHost.host.network.connect(managed.host.peerId, managed.host.listenAddress).get(5, TimeUnit.SECONDS)

        managed.waggle.subscribe(topic).get()
        managed.publish(TestShard(6, 0xFF))
        // The raw application runs its own strategy: on hearing of the object it asks for everything.
        rawWaggle.subscribeRaw(topic) { peer, _, objectId, _, pieces ->
            if (pieces != null) got += pieces else rawWaggle.send(topic, peer, objectId, byteArrayOf(0), null)
        }.get()

        waitFor("pieces from the managed node") { got.isNotEmpty() }
        assertThat(got.first()[0]).isEqualTo(0xFF.toByte())
    }

    @Test
    fun `a full connection queue rejects the publish and a later publish delivers`() {
        // A one-byte budget admits nothing: every send is refused before it reaches the stream.
        val a = Node(WaggleConfig(maxQueuedBytesPerConnection = 1))
        val b = Node()
        a.connect(b)
        subscribeBoth(a, b)

        val shard = TestShard(9, 0xFF)
        val result = runCatching { a.waggle.publish(topic, shard).get(5, TimeUnit.SECONDS) }
        assertThat(result.exceptionOrNull()?.cause).isInstanceOf(NotSentException::class.java)
        Thread.sleep(200)
        assertThat(b.bits(9)).isEqualTo(0)
    }
}
