package com.viadeo.axonframework.eventhandling.terminal.kafka;

import java.text.Normalizer;

public class TopicNormalizer {

    public String normalize(final String topic) {
        return Normalizer
                .normalize(topic, Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", "")
                .replaceAll("\\$", "_");
    }

}
