package com.streamcraft.service.pipeline.web;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@Controller
public class MonitorPageController {

    @GetMapping("/pipelines/{id}/monitor")
    public String monitorDetail(@PathVariable Long id, Model model) {
        model.addAttribute("pipelineId", id);
        return "pipeline-monitor-detail";
    }
}
