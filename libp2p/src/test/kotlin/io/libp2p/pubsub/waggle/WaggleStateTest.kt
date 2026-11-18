package io.libp2p.pubsub.waggle

import io.libp2p.core.PeerId
import io.libp2p.etc.types.WBytes
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class WaggleStateTest {
    private val topic = WBytes("topic".toByteArray())
    private val obj = WBytes(byteArrayOf(1))
    private val peer = PeerId.random()
    private val conn = StubConnection()
    private val state = WaggleState(maxCachedObjects = 2)

    @Test
    fun `store reports whether the snapshot changed`() {
        assertThat(state.store(topic, TestShard(1, 0b0011))).isTrue()
        assertThat(state.store(topic, TestShard(1, 0b0011))).isFalse()
        assertThat(state.store(topic, TestShard(1, 0b0111))).isTrue()
    }

    @Test
    fun `an unknown peer is sent everything with our metadata`() {
        state.store(topic, TestShard(1, 0b0101))
        val t = state.prepare(topic, conn, peer, obj)!!
        assertThat(t.pieces).containsExactly(0b0101, 0, 2)
        assertThat(t.metadata).containsExactly(0b0101)
    }

    @Test
    fun `nothing is sent twice once committed`() {
        state.store(topic, TestShard(1, 0b0101))
        state.commit(state.prepare(topic, conn, peer, obj)!!)
        assertThat(state.prepare(topic, conn, peer, obj)).isNull()
    }

    @Test
    fun `an uncommitted transmission is offered again`() {
        state.store(topic, TestShard(1, 0b0101))
        state.prepare(topic, conn, peer, obj)!! // queue was full: never committed
        val retry = state.prepare(topic, conn, peer, obj)!!
        assertThat(retry.pieces).containsExactly(0b0101, 0, 2)
    }

    @Test
    fun `an updated snapshot sends only the new pieces`() {
        state.store(topic, TestShard(1, 0b0101))
        state.commit(state.prepare(topic, conn, peer, obj)!!)
        state.store(topic, TestShard(1, 0b1101))
        val t = state.prepare(topic, conn, peer, obj)!!
        assertThat(t.pieces).containsExactly(0b1000, 3)
        assertThat(t.metadata).containsExactly(0b1101)
    }

    @Test
    fun `a metadata advert does not mark pieces as delivered`() {
        state.store(topic, TestShard(1, 0b0101))
        val advert = state.prepare(topic, conn, peer, obj, withPieces = false)!!
        assertThat(advert.pieces).isNull()
        assertThat(advert.metadata).containsExactly(0b0101)
        state.commit(advert)
        // Same metadata again: nothing to advertise.
        assertThat(state.prepare(topic, conn, peer, obj, withPieces = false)).isNull()
        // But the pieces are still owed.
        assertThat(state.prepare(topic, conn, peer, obj)!!.pieces).containsExactly(0b0101, 0, 2)
    }

    @Test
    fun `received for an unknown object is surfaced without a reply`() {
        val r = state.received(topic, conn, peer, obj, byteArrayOf(0b0001), null)!!
        assertThat(r.emit).isTrue()
        assertThat(r.reply).isNull()
    }

    @Test
    fun `received replies with what the peer lacks and surfaces what we lack`() {
        state.store(topic, TestShard(1, 0b0101))
        val r = state.received(topic, conn, peer, obj, byteArrayOf(0b0011), null)!!
        assertThat(r.emit).isTrue() // the peer holds piece 1
        assertThat(r.reply!!.pieces).containsExactly(0b0100, 2)
        assertThat(r.reply!!.metadata).containsExactly(0b0101)
    }

    @Test
    fun `a repeated advert is ignored`() {
        state.store(topic, TestShard(1, 0b0111))
        state.commit(state.received(topic, conn, peer, obj, byteArrayOf(0b0011), null)!!.reply!!)
        assertThat(state.received(topic, conn, peer, obj, byteArrayOf(0b0011), null)).isNull()
    }

    @Test
    fun `pieces are surfaced even without metadata`() {
        state.store(topic, TestShard(1, 0b0001))
        val r = state.received(topic, conn, peer, obj, null, byteArrayOf(0b0010, 1))!!
        assertThat(r.emit).isTrue()
    }

    @Test
    fun `adverts after a send merge into what we believe the peer holds`() {
        state.store(topic, TestShard(1, 0b0111))
        state.commit(state.prepare(topic, conn, peer, obj)!!) // we believe the peer holds 0b0111
        // An older advert from the peer must not make us resend.
        val r = state.received(topic, conn, peer, obj, byteArrayOf(0b0001), null)
        assertThat(r).isNull()
    }

    @Test
    fun `invalid metadata is reported`() {
        state.store(topic, TestShard(1, 0b0001))
        assertThatThrownBy { state.received(topic, conn, peer, obj, byteArrayOf(1, 2), null) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `the cache is a bounded LRU`() {
        state.store(topic, TestShard(1, 1))
        state.store(topic, TestShard(2, 1))
        state.cached(topic, WBytes(byteArrayOf(1))) // touch 1 so 2 is the eldest
        state.store(topic, TestShard(3, 1))
        assertThat(state.cached(topic, WBytes(byteArrayOf(2)))).isNull()
        assertThat(state.cached(topic, WBytes(byteArrayOf(1)))).isNotNull()
    }

    @Test
    fun `closing a connection forgets its view`() {
        state.store(topic, TestShard(1, 0b0101))
        state.commit(state.prepare(topic, conn, peer, obj)!!)
        state.connectionClosed(conn)
        assertThat(state.prepare(topic, conn, peer, obj)!!.pieces).containsExactly(0b0101, 0, 2)
    }
}
