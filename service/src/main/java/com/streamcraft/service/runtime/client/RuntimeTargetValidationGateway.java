package com.streamcraft.service.runtime.client;

public interface RuntimeTargetValidationGateway {

    StandaloneValidationResponse validateStandalone(String jobManagerUrl);
}
