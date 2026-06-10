package com.streamcraft.service.overview.web;

import com.streamcraft.service.overview.service.OverviewService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OverviewApiController {

    private final OverviewService overviewService;

    public OverviewApiController(OverviewService overviewService) {
        this.overviewService = overviewService;
    }

    @GetMapping("/api/overview")
    public OverviewResponse getOverview() {
        return overviewService.getOverview();
    }
}
