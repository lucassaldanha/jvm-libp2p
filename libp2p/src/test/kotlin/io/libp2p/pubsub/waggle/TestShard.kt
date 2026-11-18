package io.libp2p.pubsub.waggle

import io.libp2p.core.PeerId

/** One-byte bitmap metadata: bit i set when piece i is held. */
class TestBitmap(var bits: Int) : WaggleMetadata {
    override fun bytes() = byteArrayOf(bits.toByte())
    override fun update(remote: ByteArray): Boolean {
        require(remote.size == 1) { "bad metadata" }
        val before = bits
        bits = bits or (remote[0].toInt() and 0xFF)
        return bits != before
    }
}

/** Up to 8 pieces; piece i is the single byte i. Payload: bitmap byte, then the pieces in order. */
class TestShard(val id: Byte, val bits: Int) : WaggleShard {
    override fun objectId() = byteArrayOf(id)
    override fun metadata(): WaggleMetadata = TestBitmap(bits)

    override fun actionFor(peer: PeerId, peerMetadata: ByteArray?): WaggleAction {
        peerMetadata?.also { require(it.size == 1) { "bad metadata" } }
        val theirs = peerMetadata?.let { it[0].toInt() and 0xFF } ?: 0
        val need = theirs and bits.inv() and 0xFF != 0
        val toSend = bits and theirs.inv() and 0xFF
        if (toSend == 0) return WaggleAction(need, null)
        val payload = byteArrayOf(toSend.toByte()) + (0 until 8).filter { toSend and (1 shl it) != 0 }.map { it.toByte() }
        return WaggleAction(need, PieceSend(payload, TestBitmap(theirs or toSend)))
    }

    fun withPieces(payload: ByteArray) = TestShard(id, bits or (payload[0].toInt() and 0xFF))
}
