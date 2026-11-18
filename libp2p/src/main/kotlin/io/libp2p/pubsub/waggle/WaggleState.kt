package io.libp2p.pubsub.waggle

import io.libp2p.core.Connection
import io.libp2p.core.PeerId
import io.libp2p.etc.types.WBytes

/**
 * The reconciliation engine: a per-topic LRU of local snapshots plus, per (topic, connection,
 * object), what we believe the peer holds and what the peer last saw of us.
 *
 * Pure: no I/O, no threads. Not thread safe; [Waggle] confines it to its event thread.
 *
 * Follows sigp/waggle `shard.rs` with two deliberate differences taken from the Nim port:
 *  - a [Transmission] is prepared first and only [commit]ted once the message was admitted to
 *    the connection's queue, so a dropped send does not mark pieces as delivered;
 *  - a metadata-only advert never advances our belief about the pieces the peer holds.
 */
class WaggleState(private val maxCachedObjects: Int) {

    internal class RemoteView {
        /** What the peer holds: raw bytes it advertised, or our own optimistic model after a send. */
        var peerRaw: ByteArray? = null
        var peerModel: WaggleMetadata? = null

        /** Our metadata as the peer last saw it. */
        var ourMetadata: ByteArray? = null

        fun peerBytes(): ByteArray? = peerModel?.bytes() ?: peerRaw
    }

    /** One message for one connection. Send it, then [commit] it. */
    class Transmission internal constructor(
        val objectId: ByteArray,
        val pieces: ByteArray?,
        val metadata: ByteArray?,
        private val view: RemoteView,
        private val peerMetadataAfter: WaggleMetadata?,
        private val ourMetadataAfter: ByteArray
    ) {
        internal fun apply() {
            peerMetadataAfter?.also {
                view.peerModel = it
                view.peerRaw = null
            }
            if (metadata != null) view.ourMetadata = ourMetadataAfter
        }
    }

    class Received(val emit: Boolean, val reply: Transmission?)

    private val local = HashMap<WBytes, LinkedHashMap<WBytes, WaggleShard>>()
    private val remote = HashMap<WBytes, HashMap<Connection, HashMap<WBytes, RemoteView>>>()

    private fun view(topic: WBytes, connection: Connection, objectId: WBytes): RemoteView =
        remote.getOrPut(topic) { HashMap() }
            .getOrPut(connection) { HashMap() }
            .getOrPut(objectId) { RemoteView() }

    fun cached(topic: WBytes, objectId: WBytes): WaggleShard? = local[topic]?.get(objectId)

    fun cachedObjects(topic: WBytes): List<WBytes> = local[topic]?.keys?.toList() ?: emptyList()

    /**
     * Caches [shard] as the current snapshot of its object.
     * @return true if the object is new or its metadata changed
     */
    fun store(topic: WBytes, shard: WaggleShard): Boolean {
        val cache = local.getOrPut(topic) { LinkedHashMap(16, 0.75f, true) }
        val objectId = WBytes(shard.objectId())
        val existing = cache[objectId]
        val changed = existing == null || !existing.metadata().bytes().contentEquals(shard.metadata().bytes())
        cache[objectId] = shard
        while (cache.size > maxCachedObjects) {
            val eldest = cache.keys.first()
            cache.remove(eldest)
            remote[topic]?.values?.forEach { it.remove(eldest) }
        }
        return changed
    }

    /**
     * Computes what [connection] should be sent for [objectId], or null if nothing.
     * With [withPieces] false only a metadata advert is produced.
     * @throws Exception if the application rejects the tracked peer metadata
     */
    fun prepare(
        topic: WBytes,
        connection: Connection,
        peer: PeerId,
        objectId: WBytes,
        withPieces: Boolean = true
    ): Transmission? {
        val shard = cached(topic, objectId) ?: return null
        val view = view(topic, connection, objectId)
        val ours = shard.metadata().bytes()
        val send = if (withPieces) shard.actionFor(peer, connection, view.peerBytes()).send else null
        val metadataStale = !ours.contentEquals(view.ourMetadata)
        if (send == null && !metadataStale) return null
        // Pieces always travel with our metadata: a receiver decides whether to surface the
        // payload from the sender's metadata, so pieces alone can be dropped silently.
        return Transmission(objectId.array, send?.pieces, ours, view, send?.peerMetadataAfter, ours)
    }

    fun commit(transmission: Transmission) = transmission.apply()

    /**
     * Merges what a peer sent into our view of it.
     * @return null if there is nothing to do
     * @throws Exception if the application rejects the metadata
     */
    fun received(
        topic: WBytes,
        connection: Connection,
        peer: PeerId,
        objectId: WBytes,
        metadata: ByteArray?,
        pieces: ByteArray?
    ): Received? {
        val view = view(topic, connection, objectId)
        val model = view.peerModel
        val updated = when {
            metadata == null -> false
            model != null -> model.update(metadata)
            metadata.contentEquals(view.peerRaw) -> false
            else -> {
                view.peerRaw = metadata
                true
            }
        }
        if (!updated && pieces == null) return null

        val shard = cached(topic, objectId) ?: return Received(true, null)
        val action = shard.actionFor(peer, connection, view.peerBytes())
        val reply = action.send?.let {
            val ours = shard.metadata().bytes()
            Transmission(objectId.array, it.pieces, ours, view, it.peerMetadataAfter, ours)
        }
        return Received(action.need || pieces != null, reply)
    }

    fun connectionClosed(connection: Connection) {
        remote.values.forEach { it.remove(connection) }
    }

    /** Forgets what we know about [connection] for [topic] (the remote unsubscribed). */
    fun connectionUnsubscribed(topic: WBytes, connection: Connection) {
        remote[topic]?.remove(connection)
    }

    fun removeTopic(topic: WBytes) {
        local.remove(topic)
        remote.remove(topic)
    }

    fun removeObject(topic: WBytes, objectId: WBytes): Boolean {
        remote[topic]?.values?.forEach { it.remove(objectId) }
        return local[topic]?.remove(objectId) != null
    }
}
