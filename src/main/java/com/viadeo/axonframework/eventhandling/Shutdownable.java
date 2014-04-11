package com.viadeo.axonframework.eventhandling;

import java.io.IOException;

public interface Shutdownable {
    void shutdown() throws IOException;
}
