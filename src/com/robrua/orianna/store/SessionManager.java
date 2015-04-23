package com.robrua.orianna.store;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

/**
 * Handles multiple threads so that multiple threads can safely access the
 * database. Automatically closes sessions from terminated threads.
 *
 * @author Rob Rua (robrua@alumni.cmu.edu)
 */
public class SessionManager implements Closeable {
    /**
     * Closes sessions from terminated threads
     */
    private class Cleaner implements Runnable {
        private volatile boolean stopped = false;
        private Thread thread;

        @Override
        public void run() {
            thread = Thread.currentThread();

            while(!stopped) {
                for(final Thread thread : sessions.keySet()) {
                    if(!thread.isAlive()) {
                        sessions.get(thread).close();
                        sessions.remove(thread);
                    }
                }

                try {
                    Thread.sleep(checkMillis);
                }
                catch(final InterruptedException e) {
                    continue;
                }
            }
        }

        /**
         * Stops the cleaner
         */
        public void stop() {
            stopped = true;
            thread.interrupt();
        }
    }

    private final long checkMillis;
    private final Cleaner cleaner;
    private final SessionFactory factory;

    private final Map<Thread, Session> sessions;

    /**
     * @param factory
     *            the session factory
     * @param checkMillis
     *            how often to check for terminated threads and close their
     *            sessions
     */
    public SessionManager(final SessionFactory factory, final long checkMillis) {
        this.factory = factory;
        sessions = new ConcurrentHashMap<>();
        this.checkMillis = checkMillis;
        cleaner = new Cleaner();
        new Thread(cleaner).start();
    }

    @Override
    public void close() {
        cleaner.stop();
        for(final Session session : sessions.values()) {
            session.close();
        }
        factory.close();
    }

    /**
     * @return the hibernate session for the current thread
     */
    public Session getSession() {
        final Thread thread = Thread.currentThread();
        Session session = sessions.get(thread);
        if(session == null) {
            session = factory.openSession();
            sessions.put(thread, session);
        }

        return session;
    }
}
