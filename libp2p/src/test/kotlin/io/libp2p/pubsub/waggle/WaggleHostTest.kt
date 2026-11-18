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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

class WaggleHostTest {
    private val topic = "test-topic".toByteArray()
    private val hostFactory = HostFactory().also { it.muxLogLevel = null }

    /** A node whose application integrates every piece it receives and republishes, like a real client. */
    private inner class Node {
        val waggle = Waggle()
        val objects = ConcurrentHashMap<Byte, TestShard>()
        val subscribedPeers = CopyOnWriteArrayList<PeerId>()
        val unsubscribedPeers = CopyOnWriteArrayList<PeerId>()
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
                    val before = objects[objectId[0]] ?: TestShard(objectId[0], 0)
                    val after = if (pieces != null) before.withPieces(pieces) else before
                    objects[objectId[0]] = after
                    waggle.publish(topic, after)
                }

                override fun onSubscribed(peer: PeerId, connection: Connection, topic: ByteArray) {
                    subscribedPeers += peer
                }

                override fun onStreamFailed(peer: PeerId, connection: Connection, topic: ByteArray, cause: Throwable?) {
                    System.err.println("Waggle stream to $peer failed: $cause")
                    cause?.printStackTrace()
                }

                override fun onUnsubscribed(peer: PeerId, connection: Connection, topic: ByteArray) {
                    unsubscribedPeers += peer
                }
            })
            hostFactory.protocols = listOf<ProtocolBinding<Any>>(Identify(), waggle)
            host = hostFactory.createHost()
        }

        fun connect(other: Node) {
            host.host.network.connect(other.host.peerId, other.host.listenAddress).get(5, TimeUnit.SECONDS)
        }

        fun publish(shard: TestShard) {
            objects[shard.id] = shard
            waggle.publish(topic, shard)
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

    @Test
    fun `two nodes with complementary pieces both reconstruct the object`() {
        val a = Node()
        val b = Node()
        a.connect(b)
        a.waggle.subscribe(topic).get()
        b.waggle.subscribe(topic).get()
        waitFor("mutual subscription") { a.subscribedPeers.size == 1 && b.subscribedPeers.size == 1 }

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
        a.waggle.subscribe(topic).get()
        b.waggle.subscribe(topic).get()
        waitFor("mutual subscription") { a.subscribedPeers.size == 1 && b.subscribedPeers.size == 1 }

        b.waggle.unsubscribe(topic).get()
        waitFor("a sees b unsubscribe") { a.unsubscribedPeers.size == 1 }
        assertThat(a.waggle.subscribers(topic).get()).isEmpty()
        // b still serves a: a's own subscription stream is untouched.
        assertThat(b.waggle.subscribers(topic).get()).containsExactly(a.host.peerId)
    }
}
