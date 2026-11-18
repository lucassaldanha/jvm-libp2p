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
    private var time = 0L
    private val state = WaggleState(maxCachedObjects = 2, lifetimeMillis = 1000, now = { time })

    @Test
    fun `store reports whether the snapshot changed`() {
        assertThat(state.store(topic, TestShard(1, 0b0011))).isTrue()
        assertThat(state.store(topic, TestShard(1, 0b0011))).isFalse()
        assertThat(state.store(topic, TestShard(1, 0b0111))).isTrue()
    }

    @Test
    fun `an unknown peer is sent everything with our metadata`() {
        state.store(topic, TestShard(1, 0b0101))
        val t = state.prepare(topic, peer, obj)!!
        assertThat(t.pieces).containsExactly(0b0101, 0, 2)
        assertThat(t.metadata).containsExactly(0b0101)
    }

    @Test
    fun `nothing is sent twice once committed`() {
        state.store(topic, TestShard(1, 0b0101))
        state.commit(state.prepare(topic, peer, obj)!!)
        assertThat(state.prepare(topic, peer, obj)).isNull()
    }

    @Test
    fun `an uncommitted transmission is offered again`() {
        state.store(topic, TestShard(1, 0b0101))
        state.prepare(topic, peer, obj)!! // queue was full: never committed
        val retry = state.prepare(topic, peer, obj)!!
        assertThat(retry.pieces).containsExactly(0b0101, 0, 2)
    }

    @Test
    fun `an updated snapshot sends only the new pieces`() {
        state.store(topic, TestShard(1, 0b0101))
        state.commit(state.prepare(topic, peer, obj)!!)
        state.store(topic, TestShard(1, 0b1101))
        val t = state.prepare(topic, peer, obj)!!
        assertThat(t.pieces).containsExactly(0b1000, 3)
        assertThat(t.metadata).containsExactly(0b1101)
    }

    @Test
    fun `a metadata advert does not mark pieces as delivered`() {
        state.store(topic, TestShard(1, 0b0101))
        val advert = state.prepare(topic, peer, obj, withPieces = false)!!
        assertThat(advert.pieces).isNull()
        assertThat(advert.metadata).containsExactly(0b0101)
        state.commit(advert)
        // Same metadata again: nothing to advertise.
        assertThat(state.prepare(topic, peer, obj, withPieces = false)).isNull()
        // But the pieces are still owed.
        assertThat(state.prepare(topic, peer, obj)!!.pieces).containsExactly(0b0101, 0, 2)
    }

    @Test
    fun `received for an unknown object is surfaced without a reply`() {
        val r = state.received(topic, peer, obj, byteArrayOf(0b0001), null)!!
        assertThat(r.emit).isTrue()
        assertThat(r.reply).isNull()
    }

    @Test
    fun `received replies with what the peer lacks and surfaces what we lack`() {
        state.store(topic, TestShard(1, 0b0101))
        val r = state.received(topic, peer, obj, byteArrayOf(0b0011), null)!!
        assertThat(r.emit).isTrue() // the peer holds piece 1
        assertThat(r.reply!!.pieces).containsExactly(0b0100, 2)
        assertThat(r.reply!!.metadata).containsExactly(0b0101)
    }

    @Test
    fun `when only the peer has something to give we answer with our metadata once`() {
        state.store(topic, TestShard(1, 0b0001))
        val r = state.received(topic, peer, obj, byteArrayOf(0b0111), null)!!
        assertThat(r.emit).isTrue()
        assertThat(r.reply!!.pieces).isNull()
        assertThat(r.reply!!.metadata).containsExactly(0b0001)
        state.commit(r.reply!!)
        // The peer now knows what we hold: a further advert gets no second answer.
        val again = state.received(topic, peer, obj, byteArrayOf(0b1111), null)!!
        assertThat(again.reply).isNull()
    }

    @Test
    fun `a repeated advert is ignored`() {
        state.store(topic, TestShard(1, 0b0111))
        state.commit(state.received(topic, peer, obj, byteArrayOf(0b0011), null)!!.reply!!)
        assertThat(state.received(topic, peer, obj, byteArrayOf(0b0011), null)).isNull()
    }

    @Test
    fun `pieces are surfaced even without metadata`() {
        state.store(topic, TestShard(1, 0b0001))
        val r = state.received(topic, peer, obj, null, byteArrayOf(0b0010, 1))!!
        assertThat(r.emit).isTrue()
    }

    @Test
    fun `adverts after a send merge into what we believe the peer holds`() {
        state.store(topic, TestShard(1, 0b0111))
        state.commit(state.prepare(topic, peer, obj)!!) // we believe the peer holds 0b0111
        // An older advert from the peer must not make us resend.
        val r = state.received(topic, peer, obj, byteArrayOf(0b0001), null)
        assertThat(r).isNull()
    }

    @Test
    fun `invalid metadata is reported`() {
        state.store(topic, TestShard(1, 0b0001))
        assertThatThrownBy { state.received(topic, peer, obj, byteArrayOf(1, 2), null) }
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
    fun `a peer is known once it advertised or was sent to`() {
        val other = PeerId.random()
        state.store(topic, TestShard(1, 0b0101))
        assertThat(state.knows(topic, peer, obj)).isFalse()
        state.received(topic, peer, obj, byteArrayOf(0b0001), null)
        assertThat(state.knows(topic, peer, obj)).isTrue()
        state.prepare(topic, other, obj) // prepared but never committed
        assertThat(state.knows(topic, other, obj)).isFalse()
        state.commit(state.prepare(topic, other, obj)!!)
        assertThat(state.knows(topic, other, obj)).isTrue()
    }

    @Test
    fun `forgetting a peer forgets its view`() {
        state.store(topic, TestShard(1, 0b0101))
        state.commit(state.prepare(topic, peer, obj)!!)
        state.peerGone(peer)
        assertThat(state.prepare(topic, peer, obj)!!.pieces).containsExactly(0b0101, 0, 2)
    }

    @Test
    fun `a snapshot expires one lifetime after its last publish`() {
        state.store(topic, TestShard(1, 0b0101))
        time = 600
        state.store(topic, TestShard(1, 0b0101)) // republishing restarts the lifetime
        time = 1500
        assertThat(state.cached(topic, obj)).isNotNull()
        time = 1600
        assertThat(state.cached(topic, obj)).isNull()
        assertThat(state.prepare(topic, peer, obj)).isNull()
        assertThat(state.cachedObjects(topic)).isEmpty()
    }

    @Test
    fun `a peer view expires one lifetime after it last changed`() {
        state.store(topic, TestShard(1, 0b0101))
        state.commit(state.prepare(topic, peer, obj)!!)
        time = 900
        state.store(topic, TestShard(1, 0b0101))
        assertThat(state.prepare(topic, peer, obj)).isNull() // still believed delivered
        time = 1000
        assertThat(state.knows(topic, peer, obj)).isFalse()
        assertThat(state.prepare(topic, peer, obj)!!.pieces).containsExactly(0b0101, 0, 2)
    }

    @Test
    fun `sweep drops expired state and keeps the rest`() {
        state.store(topic, TestShard(1, 0b0001))
        time = 500
        state.store(topic, TestShard(2, 0b0001))
        time = 1200
        state.sweep()
        assertThat(state.cachedObjects(topic)).containsExactly(WBytes(byteArrayOf(2)))
    }
}
