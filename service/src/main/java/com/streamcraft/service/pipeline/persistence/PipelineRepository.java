package com.streamcraft.service.pipeline.persistence;

import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineRepository extends JpaRepository<Pipeline, Long> {

    List<Pipeline> findAllByOrderByUpdatedAtDesc();

    List<Pipeline> findByLastRunStatus(PipelineRunStatus lastRunStatus);
}
