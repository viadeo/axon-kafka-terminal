package com.viadeo.axonframework.eventhandling.terminal.kafka;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.io.EventMessageReader;
import org.axonframework.eventhandling.io.EventMessageWriter;
import org.axonframework.serializer.JavaSerializer;
import org.axonframework.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventMessageSerializer implements Encoder<EventMessage>, Decoder<EventMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventMessageSerializer.class);

    private Serializer serializer;

    @SuppressWarnings("unused")
    public EventMessageSerializer(final VerifiableProperties verifiableProperties){
        // /!\ Don't remove it!!!
        // this constructor is required by our connectors (producers/consumers)
        this();
    }

    public EventMessageSerializer(){
        this(new JavaSerializer());
    }

    public EventMessageSerializer(final Serializer serializer){
        this.serializer = serializer;
    }

    @Override
    public EventMessage fromBytes(final byte[] bytes) {
        try {
            final EventMessageReader in = new EventMessageReader(new DataInputStream(new ByteArrayInputStream(bytes)), serializer);
            return in.readEventMessage();
        } catch (IOException e) {
            LOGGER.error("unable to deserialize", e);
            return null;
        }
    }

    @Override
    public byte[] toBytes(EventMessage eventMessage) {
        checkNotNull(eventMessage);

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final EventMessageWriter outputStream = new EventMessageWriter(new DataOutputStream(baos), serializer);
            outputStream.writeEventMessage(eventMessage);

            return baos.toByteArray();
        } catch (IOException e) {
            LOGGER.error("unable to serialize", e);
            return null;
        }
    }
}