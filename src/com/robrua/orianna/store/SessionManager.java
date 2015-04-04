package com.robrua.orianna.store;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

/**
 * Handles multiple threads so that multiple threads can safely access the database. Automatically closes sessions from terminated threads.
 * 
 * @author Rob Rua (robrua@alumni.cmu.edu)
 */
public class SessionManager implements Closeable {
    private final Map<Thread, Session> sessions;
    private final SessionFactory factory;
    private final long checkMillis;
    private final Cleaner cleaner;
    
    /**
     * @param factory the session factory
     * @param checkMillis how often to check for terminated threads and close their sessions
     */
    public SessionManager(SessionFactory factory, long checkMillis) {
        this.factory = factory;
        this.sessions = new ConcurrentHashMap<>();
        this.checkMillis = checkMillis;
        this.cleaner = new Cleaner();
        new Thread(cleaner).start();
    }
    
    /**
     * @return the hibernate session for the current thread
     */
    public Session getSession() {
        Thread thread = Thread.currentThread();
        Session session = sessions.get(thread);
        if(session == null) {
            session = factory.openSession();
            sessions.put(thread, session);
        }
        
        return session;
    }

    @Override
    public void close() {
        cleaner.stop();
        for(Session session : sessions.values()) {
            session.close();
        }
        factory.close();
    }
    
    /**
     * Closes sessions from terminated threads
     */
    private class Cleaner implements Runnable {     
        private volatile boolean stopped = false;
        private Thread thread;
        
        /**
         * Stops the cleaner
         */
        public void stop() {
            stopped = true;
            thread.interrupt();
        }
        
        @Override
        public void run() {
            thread = Thread.currentThread();
            
            while(!stopped) {
                for(Thread thread : sessions.keySet()) {
                    if(!thread.isAlive()) {
                        sessions.get(thread).close();
                        sessions.remove(thread);
                    }
                }
                
                try {
                    Thread.sleep(checkMillis);
                }
                catch(InterruptedException e) {
                    continue;
                }
            }
        }
    }
}
