package com.streamcraft.service.runtime.persistence;

import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FlinkRuntimeTargetRepository extends JpaRepository<FlinkRuntimeTarget, Long> {
}
