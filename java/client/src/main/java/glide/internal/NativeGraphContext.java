/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import glide.api.models.exceptions.ClosingException;
import java.lang.ref.PhantomReference;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal ownership of a selected Rust GraphContext. Native identifiers never leave this class.
 * The existing GLIDE cleanup worker releases the native selection after its final Java owner dies.
 */
public final class NativeGraphContext implements AutoCloseable {
    private final GlideCoreClient owner;
    private final String name;
    private final long database;
    private final long databaseEpoch;
    private final State state;
    private final PhantomReference<Object> cleanup;

    private NativeGraphContext(GlideCoreClient owner, String name, long[] selected) {
        this.owner = owner;
        this.name = name;
        this.database = selected[1];
        this.databaseEpoch = selected[2];
        this.state = new State(selected[0]);
        this.cleanup = GlideCoreClient.registerNativeCleanup(this, state);
    }

    static NativeGraphContext select(GlideCoreClient owner, long parentHandle, String name) {
        long[] selected = selectNative(parentHandle, name);
        try {
            return new NativeGraphContext(owner, name, selected);
        } catch (RuntimeException | Error failure) {
            releaseNative(selected[0]);
            throw failure;
        }
    }

    /** The selected graph name, not a native identifier. */
    public String getName() {
        return name;
    }

    /** The database captured by the native selection. */
    public long getDatabase() {
        return database;
    }

    /** The database lifetime captured by the native selection. */
    public long getDatabaseEpoch() {
        return databaseEpoch;
    }

    void dispatch(GlideCoreClient caller, long parentHandle, long callbackId, byte[] request) {
        if (caller != owner) {
            throw new IllegalArgumentException("Graph context belongs to a different client");
        }
        // The cleaner takes this same lock. JNI clones the Rust context before releasing it.
        synchronized (state) {
            long handle = state.handle.get();
            if (handle == 0) {
                throw new ClosingException("Graph context is released");
            }
            executeNative(parentHandle, handle, callbackId, request);
        }
    }

    /** Release the internal selection; already-dispatched operations own independent Arc clones. */
    @Override
    public void close() {
        GlideCoreClient.runNativeCleanup(cleanup);
    }

    private static final class State implements Runnable {
        private final AtomicLong handle;

        private State(long handle) {
            this.handle = new AtomicLong(handle);
        }

        @Override
        public synchronized void run() {
            long selected = handle.getAndSet(0);
            if (selected != 0) {
                releaseNative(selected);
            }
        }
    }

    private static native long[] selectNative(long parentHandle, String name);

    private static native void releaseNative(long graphHandle);

    private static native void executeNative(
            long parentHandle, long graphHandle, long callbackId, byte[] request);
}
