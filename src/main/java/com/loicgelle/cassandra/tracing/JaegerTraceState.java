/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.loicgelle.cassandra.tracing;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.opentracing.Tracer;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
final class JaegerTraceState extends TraceState
{
    private static final Logger logger = LoggerFactory.getLogger(JaegerTraceState.class);
    private final Set<Future<?>> pendingFutures = ConcurrentHashMap.newKeySet();

    private Tracer tracer;
    private JaegerSpan requestSpan;
    private JaegerSpanContext requestSpanContext;
    private JaegerSpanContext rootSpanContext = null;
    private int numOngoingSubStates = 0;

    private Map<Integer, JaegerSpan> subStateSpans;
    private Set<Integer> usedSubStateIDs;
    private Random r;
    private boolean isNonLocal = false;
    
    public static int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS =
      Integer.parseInt(System.getProperty("cassandra.wait_for_tracing_events_timeout_secs", "0"));

    public JaegerTraceState(
            InetAddressAndPort coordinator,
            UUID sessionId,
            Tracing.TraceType traceType,
            Tracer tracer,
            JaegerSpanContext spanContext) {
        super(coordinator, sessionId, traceType);
        this.tracer = tracer;
        this.subStateSpans = new HashMap<>();
        this.usedSubStateIDs = new HashSet<>();
        this.r = new Random();
        if (spanContext != null) {
            this.isNonLocal = true;
            this.rootSpanContext = spanContext;
            this.requestSpan = (JaegerSpan) tracer.buildSpan("SubRequest")
                                                  .asChildOf(spanContext)
                                                  .start();
        }
        this.requestSpanContext = (spanContext == null)
                                  ? null
                                  : requestSpan.context();
    }

    @Override
    protected void traceImpl(String message) {
        if (requestSpan != null) {
            requestSpan.log(message);
        }
        
        final String threadName = Thread.currentThread().getName();
        final int elapsed = elapsed();

        executeMutation(TraceKeyspace.makeEventMutation(sessionIdBytes, message, elapsed, threadName, ttl));
    }

    public void beginRequest(String request, final InetAddress client, Map<String, String> parameters) {
        if (requestSpanContext == null) {
            requestSpan = (JaegerSpan) tracer.buildSpan(request)
                                             .start();
            this.requestSpanContext = requestSpan.context();
        }
        for (Map.Entry<String, String> e : parameters.entrySet())
            requestSpan.setTag(e.getKey(), e.getValue());
    }

    public void stopRequest() {
        if (requestSpan != null) {
            requestSpan.finish();
        }
    }

    protected synchronized int getNewSubStateImpl() {
        this.numOngoingSubStates++;
        Integer subStateID = null;
        do {
            subStateID = r.nextInt();
        } while(this.usedSubStateIDs.contains(subStateID));
        this.usedSubStateIDs.add(subStateID);

        JaegerSpan subStateSpan = (JaegerSpan) tracer.buildSpan("Run")
                                                     .asChildOf(requestSpan)
                                                     .start();
        this.subStateSpans.put(subStateID, subStateSpan);
        return subStateID;
    }
    
    protected synchronized void closeSubStateImpl(int subStateId) {
        this.numOngoingSubStates--;
        JaegerSpan subStateSpan = this.subStateSpans.get(subStateId);
        if (subStateSpan != null) {
            subStateSpan.finish();
            this.subStateSpans.remove(subStateId);
        }
        if (this.numOngoingSubStates == 0 && this.isNonLocal) {
            this.requestSpan.finish();
            this.requestSpan = (JaegerSpan) tracer.buildSpan("SubRequest")
                                                  .asChildOf(this.rootSpanContext)
                                                  .start();
        }
    }

    protected void waitForPendingEvents() {
        if (WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS <= 0) {
            stopRequest();
            return;
        }
        
        try {
            if (logger.isTraceEnabled())
                logger.trace("Waiting for up to {} seconds for {} trace events to complete",
                             +WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, pendingFutures.size());

            CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture<?>[pendingFutures.size()]))
                             .get(WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            if (logger.isTraceEnabled())
                logger.trace("Failed to wait for tracing events to complete in {} seconds",
                             WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS);
            stopRequest();
        } catch (Throwable t) {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Got exception whilst waiting for tracing events to complete", t);
        }
    }

    public JaegerSpanContext getSpanContext() {
        return requestSpanContext;
    }

    void executeMutation(final Mutation mutation)
    {
        CompletableFuture<Void> fut = CompletableFuture.runAsync(new WrappedRunnable(){
            protected void runMayThrow()
            {
                mutateWithCatch(mutation);
            }
        }, StageManager.getStage(Stage.TRACING));

        boolean ret = pendingFutures.add(fut);
        if (!ret)
            logger.warn("Failed to insert pending future, tracing synchronization may not work");
    }

    static void mutateWithCatch(Mutation mutation)
    {
        try
        {
            StorageProxy.mutate(Collections.singletonList(mutation), ConsistencyLevel.ANY, System.nanoTime());
        }
        catch (OverloadedException e)
        {
            logger.warn("Too many nodes are overloaded to save trace events");
        }
    }
}