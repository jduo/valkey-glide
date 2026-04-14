/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import glide.api.models.GlideString;
import glide.api.models.configuration.RequestRoutingConfiguration.ByAddressRoute;
import glide.api.models.configuration.RequestRoutingConfiguration.Route;
import glide.api.models.configuration.RequestRoutingConfiguration.SimpleMultiNodeRoute;
import glide.api.models.configuration.RequestRoutingConfiguration.SimpleSingleNodeRoute;
import glide.api.models.configuration.RequestRoutingConfiguration.SlotIdRoute;
import glide.api.models.configuration.RequestRoutingConfiguration.SlotKeyRoute;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Serializes command requests into a DirectByteBuffer for zero-copy JNI transfer. Format:
 *
 * <pre>
 * [4 bytes: requestType]
 * [4 bytes: argCount]
 * [4 bytes: flags] (bit 0 = expectUtf8)
 * [4 bytes: routeType] (-1 = none, 0-5 = route types)
 * [4 bytes: slotId]
 * [4 bytes: slotType]
 * [4 bytes: port]
 * [4 bytes: routeParamLen] (0 if no route param)
 * [routeParam bytes]
 * For each arg:
 *   [4 bytes: argLen]
 *   [arg bytes]
 * </pre>
 *
 * Thread-local reusable buffer avoids allocation after warmup.
 */
public class CommandBuffer {

    private static final int HEADER_SIZE = 32; // 8 ints
    private static final int INITIAL_CAPACITY = 4096;

    // Route type constants
    static final int ROUTE_NONE = -1;
    static final int ROUTE_ALL_NODES = 0;
    static final int ROUTE_ALL_PRIMARIES = 1;
    static final int ROUTE_RANDOM = 2;
    static final int ROUTE_SLOT_ID = 3;
    static final int ROUTE_SLOT_KEY = 4;
    static final int ROUTE_BY_ADDRESS = 5;

    private static final ThreadLocal<ByteBuffer> THREAD_BUFFER =
            ThreadLocal.withInitial(() -> allocateBuffer(INITIAL_CAPACITY));

    private static ByteBuffer allocateBuffer(int capacity) {
        ByteBuffer buf = ByteBuffer.allocateDirect(capacity);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return buf;
    }

    private static ByteBuffer ensureCapacity(ByteBuffer buf, int needed) {
        if (buf.remaining() >= needed) return buf;
        int newCap = Math.max(buf.capacity() * 2, buf.position() + needed);
        ByteBuffer newBuf = allocateBuffer(newCap);
        buf.flip();
        newBuf.put(buf);
        THREAD_BUFFER.set(newBuf);
        return newBuf;
    }

    /**
     * Serialize a command with String args into the thread-local DirectByteBuffer. Returns the buffer
     * positioned at 0 with limit set to the written length.
     */
    public static ByteBuffer serialize(
            int requestType, String[] args, boolean expectUtf8, Route route) {
        ByteBuffer buf = THREAD_BUFFER.get();
        buf.clear();

        // Estimate size: header + route param + args
        int estimate = HEADER_SIZE;
        for (String arg : args) {
            estimate += 4 + arg.length() * 3; // worst case UTF-8
        }
        buf = ensureCapacity(buf, estimate);

        // Write header placeholder (will fill route after)
        int headerPos = buf.position();
        buf.putInt(requestType);
        buf.putInt(args.length);
        buf.putInt(expectUtf8 ? 1 : 0);

        // Route info
        writeRoute(buf, route);

        // Args
        for (String arg : args) {
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            buf = ensureCapacity(buf, 4 + bytes.length);
            buf.putInt(bytes.length);
            buf.put(bytes);
        }

        buf.flip();
        return buf;
    }

    /** Serialize a command with GlideString args into the thread-local DirectByteBuffer. */
    public static ByteBuffer serialize(
            int requestType, GlideString[] args, boolean expectUtf8, Route route) {
        ByteBuffer buf = THREAD_BUFFER.get();
        buf.clear();

        int estimate = HEADER_SIZE;
        for (GlideString arg : args) {
            estimate += 4 + arg.getBytes().length;
        }
        buf = ensureCapacity(buf, estimate);

        buf.putInt(requestType);
        buf.putInt(args.length);
        buf.putInt(expectUtf8 ? 1 : 0);

        writeRoute(buf, route);

        for (GlideString arg : args) {
            byte[] bytes = arg.getBytes();
            buf = ensureCapacity(buf, 4 + bytes.length);
            buf.putInt(bytes.length);
            buf.put(bytes);
        }

        buf.flip();
        return buf;
    }

    private static void writeRoute(ByteBuffer buf, Route route) {
        if (route == null) {
            buf.putInt(ROUTE_NONE); // routeType
            buf.putInt(0); // slotId
            buf.putInt(0); // slotType
            buf.putInt(0); // port
            buf.putInt(0); // routeParamLen
            return;
        }

        if (route instanceof SimpleSingleNodeRoute) {
            buf.putInt(ROUTE_RANDOM);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
        } else if (route instanceof SimpleMultiNodeRoute) {
            SimpleMultiNodeRoute multi = (SimpleMultiNodeRoute) route;
            switch (multi) {
                case ALL_NODES:
                    buf.putInt(ROUTE_ALL_NODES);
                    break;
                case ALL_PRIMARIES:
                    buf.putInt(ROUTE_ALL_PRIMARIES);
                    break;
                default:
                    buf.putInt(ROUTE_NONE);
            }
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
        } else if (route instanceof SlotIdRoute) {
            SlotIdRoute sir = (SlotIdRoute) route;
            buf.putInt(ROUTE_SLOT_ID);
            buf.putInt(sir.getSlotId());
            buf.putInt(sir.getSlotType().ordinal());
            buf.putInt(0);
            buf.putInt(0);
        } else if (route instanceof SlotKeyRoute) {
            SlotKeyRoute skr = (SlotKeyRoute) route;
            byte[] keyBytes = skr.getSlotKey().getBytes(StandardCharsets.UTF_8);
            buf.putInt(ROUTE_SLOT_KEY);
            buf.putInt(0);
            buf.putInt(skr.getSlotType().ordinal());
            buf.putInt(0);
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
        } else if (route instanceof ByAddressRoute) {
            ByAddressRoute bar = (ByAddressRoute) route;
            byte[] hostBytes = bar.getHost().getBytes(StandardCharsets.UTF_8);
            buf.putInt(ROUTE_BY_ADDRESS);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(bar.getPort());
            buf.putInt(hostBytes.length);
            buf.put(hostBytes);
        } else {
            buf.putInt(ROUTE_NONE);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
        }
    }

    /**
     * Serialize a command with pre-converted byte[][] args. Used by the internal path where args are
     * already converted from String/GlideString to byte[].
     */
    public static ByteBuffer serializeRaw(
            int requestType, byte[][] args, boolean expectUtf8, Route route) {
        ByteBuffer buf = THREAD_BUFFER.get();
        buf.clear();

        int estimate = HEADER_SIZE;
        for (byte[] arg : args) {
            estimate += 4 + arg.length;
        }
        buf = ensureCapacity(buf, estimate);

        buf.putInt(requestType);
        buf.putInt(args.length);
        buf.putInt(expectUtf8 ? 1 : 0);

        writeRoute(buf, route);

        for (byte[] arg : args) {
            buf = ensureCapacity(buf, 4 + arg.length);
            buf.putInt(arg.length);
            buf.put(arg);
        }

        buf.flip();
        return buf;
    }
}
