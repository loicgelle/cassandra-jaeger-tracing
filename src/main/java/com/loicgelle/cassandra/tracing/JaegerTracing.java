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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.tracing.CassandraJaegerContext;
import org.apache.cassandra.tracing.ExpiredTraceState;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerSpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableList;

public final class JaegerTracing extends Tracing {
    private static final Tracer tracer;
    static {
        GlobalTracer.registerIfAbsent(new Callable<Tracer>() {
            @Override
            public Tracer call()
            {
                return Configuration
                    .fromEnv()
                    //.withSampler(new Configuration.SamplerConfiguration().withType("probabilistic").withParam(0.1))
                    .withSampler(new Configuration.SamplerConfiguration().withType("const").withParam(1))
                    .getTracer();
            }
        });
        tracer = GlobalTracer.get();
    }

    protected void stopSessionImpl() {
        final JaegerTraceState state = getStateImpl();
        if (state == null)
            return;
        
        int elapsed = state.elapsed();
        ByteBuffer sessionId = state.sessionIdBytes;
        int ttl = state.ttl;
        state.executeMutation(TraceKeyspace.makeStopSessionMutation(sessionId, elapsed, ttl));
        state.stopRequest();
    }

    public TraceState begin(final String request, final InetAddress client, final Map<String, String> parameters) {
        assert isTracing();

        final JaegerTraceState state = getStateImpl();
        assert state != null;

        final long startedAt = System.currentTimeMillis();
        final ByteBuffer sessionId = state.sessionIdBytes;
        final String command = state.traceType.toString();
        final int ttl = state.ttl;

        state.beginRequest(request, client, parameters);
        state.executeMutation(TraceKeyspace.makeStartSessionMutation(sessionId, client, parameters, request, startedAt, command, ttl));
        return state;
    }

    /**
     * Convert the abstract tracing state to its implementation.
     *
     * Expired states are not put in the sessions but the check is for extra safety.
     *
     * @return the state converted to its implementation, or null
     */
    private JaegerTraceState getStateImpl() {
        TraceState state = get();
        if (state == null)
            return null;
        
        if (state instanceof ExpiredTraceState) {
            ExpiredTraceState expiredTraceState = (ExpiredTraceState) state;
            state = expiredTraceState.getDelegate();
        }

        if (state instanceof JaegerTraceState) {
            return (JaegerTraceState) state;
        }

        assert false : "TracingImpl states should be of type JaegerTraceState";
        return null;
    }

    protected TraceState newTraceState(InetAddressAndPort coordinator, UUID sessionId, Tracing.TraceType traceType) {
        return new JaegerTraceState(coordinator, sessionId, traceType, tracer, null);
    }

    protected TraceState newTraceStateWithCtx(InetAddressAndPort coordinator, UUID sessionId, Tracing.TraceType traceType,
                                              JaegerSpanContext spanContext) {
        return new JaegerTraceState(coordinator, sessionId, traceType, tracer, spanContext);
    }

    public List<Object> getTraceHeaders() {
        assert isTracing();

        JaegerSpanContext spanContext = null;
        JaegerTraceState state = getStateImpl();
        if (state != null) {
            spanContext = state.getSpanContext();
        }

        if (spanContext == null)
            return ImmutableList.of(
                ParameterType.TRACE_SESSION, Tracing.instance.getSessionId(),
                ParameterType.TRACE_TYPE, Tracing.instance.getTraceType());
        else {
            CassandraJaegerContext jaegerContext =
            new CassandraJaegerContext(
                spanContext.getTraceIdHigh(),
                spanContext.getTraceIdLow(),
                spanContext.getSpanId(),
                spanContext.getParentId(),
                spanContext.getFlags()
            );
            return ImmutableList.of(
                ParameterType.TRACE_SESSION, Tracing.instance.getSessionId(),
                ParameterType.TRACE_TYPE, Tracing.instance.getTraceType(),
                ParameterType.JAEGER_CONTEXT, jaegerContext);

        }
    }

    public TraceState initializeFromMessage(final MessageIn<?> message) {
        final UUID sessionId = (UUID)message.parameters.get(ParameterType.TRACE_SESSION);
        final CassandraJaegerContext jaegerContext =
                (CassandraJaegerContext)message.parameters.get(ParameterType.JAEGER_CONTEXT);
        final JaegerSpanContext jaegerSpanContext = (jaegerContext == null)
                ? null
                : (JaegerSpanContext) new JaegerSpanContext(jaegerContext.traceIdHigh,
                                                            jaegerContext.traceIdLow,
                                                            jaegerContext.spanId,
                                                            jaegerContext.parentId,
                                                            jaegerContext.flags);

        if (sessionId == null)
            return null;

        TraceState ts = get(sessionId);
        if (ts != null && ts.acquireReference())
            return ts;

        TraceType tmpType;
        TraceType traceType = TraceType.QUERY;
        if ((tmpType = (TraceType)message.parameters.get(ParameterType.TRACE_TYPE)) != null)
            traceType = tmpType;

        if (message.verb == MessagingService.Verb.REQUEST_RESPONSE)
        {
            // received a message for a session we've already closed out.  see CASSANDRA-5668
            return new ExpiredTraceState(newTraceStateWithCtx(message.from,
                                                              sessionId,
                                                              traceType,
                                                              jaegerSpanContext));
        }
        else
        {
            ts = newTraceStateWithCtx(message.from, sessionId, traceType, jaegerSpanContext);
            sessions.put(sessionId, ts);
            return ts;
        }
    }

    /**
     * Called for non-local traces (traces that are not initiated by local node == coordinator).
     */
    public void trace(ByteBuffer sessionId, String message, int ttl) {
        // TODO
    }
}