package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.streamcraft.service.pipeline.model.PipelineRunClaim;
import com.streamcraft.service.pipeline.model.PipelineRunClaimStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRunClaimRepository;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PipelineRunClaimServiceTest {

    private PipelineRunClaimRepository repository;
    private PipelineRunClaimService service;

    @BeforeEach
    void setUp() {
        repository = mock(PipelineRunClaimRepository.class);
        service = new PipelineRunClaimService(repository);
        when(repository.saveAndFlush(any(PipelineRunClaim.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void rejectsAnExistingActiveClaim() {
        PipelineRunClaim existing = claim(7L, 3L, PipelineRunClaimStatus.RUNNING);
        when(repository.findByPipelineId(42L)).thenReturn(Optional.of(existing));

        PipelineRunClaimService.ClaimConflictException exception = assertThrows(
                PipelineRunClaimService.ClaimConflictException.class,
                () -> service.claim(42L, "job-2", "owner-2"));

        assertSame(existing, exception.claim());
    }

    @Test
    void reusesAReleasedRowWithAnewOwnerAndJob() {
        PipelineRunClaim existing = claim(7L, 3L, PipelineRunClaimStatus.RELEASED);
        when(repository.findByPipelineId(42L)).thenReturn(Optional.of(existing));

        PipelineRunClaim result = service.claim(42L, "job-2", "owner-2");

        assertSame(existing, result);
        assertEquals(42L, result.getPipelineId());
        assertEquals("job-2", result.getJobId());
        assertEquals("owner-2", result.getOwnerToken());
        assertEquals(PipelineRunClaimStatus.SUBMITTING, result.getStatus());
        assertEquals(null, result.getLastError());
    }

    @Test
    void releaseAfterTerminalRequiresTheExpectedVersion() {
        PipelineRunClaim expected = claim(7L, 3L, PipelineRunClaimStatus.RUNNING);
        PipelineRunClaim current = claim(7L, 4L, PipelineRunClaimStatus.RUNNING);
        when(repository.findById(7L)).thenReturn(Optional.of(current));

        assertThrows(
                PipelineRunClaimService.ClaimConflictException.class,
                () -> service.releaseAfterTerminal(expected, "FLINK_JOB_TERMINAL"));
        assertEquals(PipelineRunClaimStatus.RUNNING, current.getStatus());
    }

    @Test
    void submitFailureMakesTheClaimReusable() {
        PipelineRunClaim claim = claim(7L, 3L, PipelineRunClaimStatus.SUBMITTING);
        when(repository.findById(7L)).thenReturn(Optional.of(claim));

        service.releaseAfterSubmitFailure(claim, "broker rejected");

        assertEquals(PipelineRunClaimStatus.FAILED, claim.getStatus());
        assertEquals("broker rejected", claim.getLastError());
    }

    private PipelineRunClaim claim(Long id, Long version, PipelineRunClaimStatus status) {
        PipelineRunClaim claim = new PipelineRunClaim();
        claim.setId(id);
        claim.setPipelineId(42L);
        claim.setJobId("job-1");
        claim.setOwnerToken("owner-1");
        claim.setVersion(version);
        claim.setStatus(status);
        return claim;
    }
}
