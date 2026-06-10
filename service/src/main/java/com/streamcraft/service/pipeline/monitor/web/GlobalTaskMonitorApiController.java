package com.streamcraft.service.pipeline.monitor.web;

import com.streamcraft.service.pipeline.monitor.service.GlobalTaskMonitorService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GlobalTaskMonitorApiController {

    private final GlobalTaskMonitorService globalTaskMonitorService;

    public GlobalTaskMonitorApiController(GlobalTaskMonitorService globalTaskMonitorService) {
        this.globalTaskMonitorService = globalTaskMonitorService;
    }

    @GetMapping("/api/pipelines/monitor")
    public GlobalTaskMonitorResponse getMonitor() {
        return globalTaskMonitorService.getMonitor();
    }
}
