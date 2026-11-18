package io.libp2p.pubsub.waggle

import io.libp2p.core.PeerId
import io.libp2p.etc.types.WBytes

/**
 * The reconciliation engine: a per-topic LRU of local snapshots plus, per (topic, peer, object),
 * what we believe the peer holds and what the peer last saw of us.
 *
 * Pure: no I/O, no threads, time comes from [now]. Not thread safe; [Waggle] confines it to its
 * event thread.
 *
 * Follows sigp/waggle `shard.rs`, with these deliberate differences (the first three are from
 * the Nim port):
 *  - a [Transmission] is prepared first and only [commit]ted once the message was admitted to
 *    the connection's queue, so a dropped send does not mark pieces as delivered;
 *  - a metadata-only advert never advances our belief about the pieces the peer holds;
 *  - snapshots and peer views expire;
 *  - views are per peer, not per connection, because [Waggle] subscribes on one connection per peer;
 *  - when a peer advertises something we lack and has not seen our current metadata, we answer
 *    with our metadata so that it can send us what we are missing.
 */
class WaggleState(
    private val maxCachedObjects: Int,
    private val lifetimeMillis: Long = Long.MAX_VALUE,
    private val now: () -> Long = System::currentTimeMillis
) {

    internal class RemoteView(var touchedAt: Long) {
        /** What the peer holds: raw bytes it advertised, or our own optimistic model after a send. */
        var peerRaw: ByteArray? = null
        var peerModel: WaggleMetadata? = null

        /** Our metadata as the peer last saw it. */
        var ourMetadata: ByteArray? = null

        fun peerBytes(): ByteArray? = peerModel?.bytes() ?: peerRaw
    }

    private class Entry(val shard: WaggleShard, val publishedAt: Long)

    /** One message for one peer. Send it, then [commit] it. */
    class Transmission internal constructor(
        val objectId: ByteArray,
        val pieces: ByteArray?,
        val metadata: ByteArray?,
        private val view: RemoteView,
        private val peerMetadataAfter: WaggleMetadata?,
        private val touchedAt: Long
    ) {
        internal fun apply() {
            peerMetadataAfter?.also {
                view.peerModel = it
                view.peerRaw = null
            }
            if (metadata != null) view.ourMetadata = metadata
            view.touchedAt = touchedAt
        }
    }

    class Received(val emit: Boolean, val reply: Transmission?)

    private val local = HashMap<WBytes, LinkedHashMap<WBytes, Entry>>()
    private val remote = HashMap<WBytes, HashMap<PeerId, HashMap<WBytes, RemoteView>>>()

    private fun expired(since: Long) = now() - since >= lifetimeMillis

    private fun view(topic: WBytes, peer: PeerId, objectId: WBytes): RemoteView {
        val views = remote.getOrPut(topic) { HashMap() }.getOrPut(peer) { HashMap() }
        val existing = views[objectId]
        if (existing != null && !expired(existing.touchedAt)) return existing
        return RemoteView(now()).also { views[objectId] = it }
    }

    /** True if we track anything about [peer] for this object: it advertised to us, or we sent to it. */
    fun knows(topic: WBytes, peer: PeerId, objectId: WBytes): Boolean {
        val v = remote[topic]?.get(peer)?.get(objectId) ?: return false
        return !expired(v.touchedAt) && (v.peerBytes() != null || v.ourMetadata != null)
    }

    fun cached(topic: WBytes, objectId: WBytes): WaggleShard? {
        val cache = local[topic] ?: return null
        val entry = cache[objectId] ?: return null
        if (expired(entry.publishedAt)) {
            cache.remove(objectId)
            return null
        }
        return entry.shard
    }

    fun cachedObjects(topic: WBytes): List<WBytes> =
        local[topic]?.filterValues { !expired(it.publishedAt) }?.keys?.toList() ?: emptyList()

    /**
     * Caches [shard] as the current snapshot of its object and restarts its lifetime.
     * @return true if the object is new or its metadata changed
     */
    fun store(topic: WBytes, shard: WaggleShard): Boolean {
        val cache = local.getOrPut(topic) { LinkedHashMap(16, 0.75f, true) }
        val objectId = WBytes(shard.objectId())
        val existing = cached(topic, objectId)
        val changed = existing == null || !existing.metadata().bytes().contentEquals(shard.metadata().bytes())
        cache[objectId] = Entry(shard, now())
        while (cache.size > maxCachedObjects) {
            val eldest = cache.keys.first()
            cache.remove(eldest)
            remote[topic]?.values?.forEach { it.remove(eldest) }
        }
        return changed
    }

    /**
     * Computes what [peer] should be sent for [objectId], or null if nothing.
     * With [withPieces] false only a metadata advert is produced.
     * @throws Exception if the application rejects the tracked peer metadata
     */
    fun prepare(topic: WBytes, peer: PeerId, objectId: WBytes, withPieces: Boolean = true): Transmission? {
        val shard = cached(topic, objectId) ?: return null
        val view = view(topic, peer, objectId)
        val ours = shard.metadata().bytes()
        val send = if (withPieces) shard.actionFor(peer, view.peerBytes()).send else null
        val metadataStale = !ours.contentEquals(view.ourMetadata)
        if (send == null && !metadataStale) return null
        // Pieces always travel with our metadata: a receiver decides whether to surface the
        // payload from the sender's metadata, so pieces alone can be dropped silently.
        return Transmission(objectId.array, send?.pieces, ours, view, send?.peerMetadataAfter, now())
    }

    fun commit(transmission: Transmission) = transmission.apply()

    /**
     * Merges what a peer sent into our view of it.
     * @return null if there is nothing to do
     * @throws Exception if the application rejects the metadata
     */
    fun received(topic: WBytes, peer: PeerId, objectId: WBytes, metadata: ByteArray?, pieces: ByteArray?): Received? {
        val view = view(topic, peer, objectId)
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
        if (updated) view.touchedAt = now()
        if (!updated && pieces == null) return null

        val shard = cached(topic, objectId) ?: return Received(true, null)
        val action = shard.actionFor(peer, view.peerBytes())
        val ours = shard.metadata().bytes()
        val send = action.send
        val reply = when {
            send != null -> Transmission(objectId.array, send.pieces, ours, view, send.peerMetadataAfter, now())
            // The peer has something we lack and does not know what we hold: tell it, so it can serve us.
            action.need && !ours.contentEquals(view.ourMetadata) -> Transmission(objectId.array, null, ours, view, null, now())
            else -> null
        }
        return Received(action.need || pieces != null, reply)
    }

    /** Drops expired snapshots and peer views. Lookups already ignore them; this frees the memory. */
    fun sweep() {
        local.values.forEach { cache -> cache.values.removeIf { expired(it.publishedAt) } }
        local.values.removeIf { it.isEmpty() }
        remote.values.forEach { peers ->
            peers.values.forEach { views -> views.values.removeIf { expired(it.touchedAt) } }
            peers.values.removeIf { it.isEmpty() }
        }
        remote.values.removeIf { it.isEmpty() }
    }

    /** Forgets everything about [peer] (its last connection closed). */
    fun peerGone(peer: PeerId) {
        remote.values.forEach { it.remove(peer) }
    }

    /** Forgets what we know about [peer] for [topic] (it unsubscribed). */
    fun peerUnsubscribed(topic: WBytes, peer: PeerId) {
        remote[topic]?.remove(peer)
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
