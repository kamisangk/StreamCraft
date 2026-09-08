package com.streamcraft.service.pipeline.persistence;

import com.streamcraft.service.pipeline.model.PipelineRunClaim;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineRunClaimRepository extends JpaRepository<PipelineRunClaim, Long> {

    Optional<PipelineRunClaim> findByPipelineId(Long pipelineId);
}
