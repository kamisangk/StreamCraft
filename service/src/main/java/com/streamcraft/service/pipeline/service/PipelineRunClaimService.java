package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.pipeline.model.PipelineRunClaim;
import com.streamcraft.service.pipeline.model.PipelineRunClaimStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRunClaimRepository;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Owns durable claim transitions and fencing-token checks for Exactly-Once runs. */
@Service
public class PipelineRunClaimService {

    private final PipelineRunClaimRepository repository;

    public PipelineRunClaimService(PipelineRunClaimRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public Optional<PipelineRunClaim> find(Long pipelineId) {
        return repository.findByPipelineId(pipelineId);
    }

    @Transactional
    public PipelineRunClaim claim(Long pipelineId, String jobId, String ownerToken) {
        requireText(jobId, "jobId");
        requireText(ownerToken, "owner token");

        PipelineRunClaim claim = repository.findByPipelineId(pipelineId).orElseGet(PipelineRunClaim::new);
        if (claim.getId() != null && claim.isActive()) {
            throw new ClaimConflictException(claim);
        }

        claim.setPipelineId(pipelineId);
        claim.setJobId(jobId);
        claim.setOwnerToken(ownerToken);
        claim.setStatus(PipelineRunClaimStatus.SUBMITTING);
        claim.setClaimedAt(java.time.Instant.now());
        claim.setLastError(null);
        try {
            return repository.saveAndFlush(claim);
        } catch (DataIntegrityViolationException | OptimisticLockingFailureException exception) {
            throw new ClaimConflictException(null, exception);
        }
    }

    @Transactional
    public void markSubmitted(PipelineRunClaim expected) {
        PipelineRunClaim claim = ownedActiveClaim(expected);
        if (claim.getStatus() != PipelineRunClaimStatus.SUBMITTING) {
            throw new IllegalStateException("Pipeline run claim is no longer submitting.");
        }
        claim.setStatus(PipelineRunClaimStatus.RUNNING);
        claim.setLastError(null);
        repository.saveAndFlush(claim);
    }

    @Transactional
    public void recordUncertainSubmission(PipelineRunClaim expected, String message) {
        PipelineRunClaim claim = ownedActiveClaim(expected);
        claim.setLastError(limitError(message));
        repository.saveAndFlush(claim);
    }

    @Transactional
    public void releaseAfterSubmitFailure(PipelineRunClaim expected, String message) {
        PipelineRunClaim claim = ownedActiveClaim(expected);
        claim.setStatus(PipelineRunClaimStatus.FAILED);
        claim.setLastError(limitError(message));
        repository.saveAndFlush(claim);
    }

    /** Releases a previous active claim only after an explicit terminal Flink result. */
    @Transactional
    public void releaseAfterTerminal(PipelineRunClaim expected, String reason) {
        PipelineRunClaim claim = repository.findById(expected.getId())
                .orElseThrow(() -> new ClaimConflictException(null));
        verifyVersion(expected, claim);
        if (!claim.isActive()) {
            return;
        }
        claim.setStatus(PipelineRunClaimStatus.RELEASED);
        claim.setLastError(limitError(reason));
        try {
            repository.saveAndFlush(claim);
        } catch (OptimisticLockingFailureException exception) {
            throw new ClaimConflictException(null, exception);
        }
    }

    private PipelineRunClaim ownedActiveClaim(PipelineRunClaim expected) {
        if (expected == null || expected.getId() == null) {
            throw new IllegalArgumentException("Pipeline run claim is required.");
        }
        PipelineRunClaim claim = repository.findById(expected.getId())
                .orElseThrow(() -> new IllegalStateException("Pipeline run claim no longer exists."));
        verifyVersion(expected, claim);
        if (!java.util.Objects.equals(expected.getOwnerToken(), claim.getOwnerToken())) {
            throw new IllegalStateException("Pipeline run claim owner token does not match.");
        }
        if (!claim.isActive()) {
            throw new IllegalStateException("Pipeline run claim is no longer active.");
        }
        return claim;
    }

    private void verifyVersion(PipelineRunClaim expected, PipelineRunClaim actual) {
        if (expected.getVersion() != null
                && actual.getVersion() != null
                && !expected.getVersion().equals(actual.getVersion())) {
            throw new ClaimConflictException(actual);
        }
    }

    private String limitError(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 1000 ? message : message.substring(0, 1000);
    }

    private void requireText(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Pipeline run claim " + label + " must not be blank.");
        }
    }

    public static final class ClaimConflictException extends IllegalStateException {

        private final PipelineRunClaim claim;

        public ClaimConflictException(PipelineRunClaim claim) {
            this(claim, null);
        }

        public ClaimConflictException(PipelineRunClaim claim, Throwable cause) {
            super("Pipeline already has an active Exactly-Once run.", cause);
            this.claim = claim;
        }

        public PipelineRunClaim claim() {
            return claim;
        }
    }
}
