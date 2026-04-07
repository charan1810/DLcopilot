import { useCallback, useEffect, useState } from "react";
import {
    fetchPipelineRuns,
    fetchPipelineRun,
    retryPipelineRun,
    fixSqlQuery,
    updatePipelineStep,
} from "../api/schemaApi";

function statusClass(status) {
    const key = String(status || "").toLowerCase();
    if (key === "success") return "run-status-success";
    if (key === "failed") return "run-status-failed";
    if (key === "running" || key === "pending") return "run-status-running";
    if (key === "partial_success") return "run-status-partial_success";
    return "run-status-running";
}

function formatDuration(seconds) {
    if (seconds == null) return "-";
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const mins = Math.floor(seconds / 60);
    const secs = (seconds % 60).toFixed(0);
    return `${mins}m ${secs}s`;
}

function formatTimestamp(ts) {
    if (!ts) return "-";
    try {
        const d = new Date(ts);
        return d.toLocaleString();
    } catch {
        return ts;
    }
}

const SQL_BLOCK_COMMENT_RE = /\/\*[\s\S]*?\*\//g;
const SQL_LINE_COMMENT_RE = /--[^\n\r]*/g;
const DDL_KEYWORDS = new Set(["create", "alter", "drop", "truncate"]);

function getSqlLeadingVerb(sqlText = "") {
    const withoutBlock = sqlText.replace(SQL_BLOCK_COMMENT_RE, " ");
    const withoutLine = withoutBlock.replace(SQL_LINE_COMMENT_RE, " ");
    const trimmed = withoutLine.trimStart();
    if (!trimmed) return "";
    return (trimmed.split(/\s+/, 1)[0] || "").toLowerCase();
}

export default function PipelineRunsTab({
    pipelineId,
    pipelineName,
    connectionId,
    databaseName,
    selectedPipeline,
    setSelectedPipeline,
    onMessage,
    onError,
}) {
    const [runs, setRuns] = useState([]);
    const [selectedRun, setSelectedRun] = useState(null);
    const [loading, setLoading] = useState(false);
    const [retrying, setRetrying] = useState(false);
    const [aiFixes, setAiFixes] = useState({});
    const [livePolling, setLivePolling] = useState(false);

    // Reset selection when pipeline changes
    useEffect(() => {
        setSelectedRun(null);
        setAiFixes({});
        setRuns([]);
    }, [pipelineId]);

    const loadRuns = useCallback(async () => {
        if (!pipelineId) return;
        try {
            setLoading(true);
            const data = await fetchPipelineRuns(pipelineId);
            const list = Array.isArray(data) ? data : [];
            setRuns(list);
            return list;
        } catch (err) {
            onError?.(err.message || "Failed to load runs");
            return [];
        } finally {
            setLoading(false);
        }
    }, [pipelineId, onError]);

    // Load runs and auto-open the latest
    useEffect(() => {
        let cancelled = false;
        (async () => {
            const list = await loadRuns();
            if (!cancelled && list.length > 0 && !selectedRun) {
                handleOpenRun(list[0].id);
            }
        })();
        return () => { cancelled = true; };
    }, [loadRuns]);

    // Poll runs list every 15s to catch scheduled runs
    useEffect(() => {
        if (!pipelineId) return;
        setLivePolling(true);
        const id = setInterval(() => {
            loadRuns();
        }, 15000);
        return () => {
            clearInterval(id);
            setLivePolling(false);
        };
    }, [pipelineId, loadRuns]);

    // Poll selected run every 5s while it is RUNNING or PENDING
    useEffect(() => {
        const status = (selectedRun?.status || "").toUpperCase();
        if (status !== "RUNNING" && status !== "PENDING") return;
        const id = setInterval(async () => {
            try {
                const run = await fetchPipelineRun(selectedRun.id);
                setSelectedRun(run);
                const newStatus = (run?.status || "").toUpperCase();
                if (newStatus !== "RUNNING" && newStatus !== "PENDING") {
                    // Run finished — also refresh the list
                    loadRuns();
                }
            } catch { /* silent */ }
        }, 5000);
        return () => clearInterval(id);
    }, [selectedRun?.id, selectedRun?.status, loadRuns]);

    async function handleOpenRun(runId) {
        try {
            const run = await fetchPipelineRun(runId);
            setSelectedRun(run);
            setAiFixes({});
        } catch (err) {
            onError?.(err.message || "Failed to load run details");
        }
    }

    async function handleRetryRun() {
        if (!selectedRun) return;
        try {
            setRetrying(true);
            onError?.("");
            onMessage?.("");
            const result = await retryPipelineRun(selectedRun.id);
            // Fetch full run details (retry may return summary only)
            const newRunId = result.new_run_id || result.id;
            const fullRun = await fetchPipelineRun(newRunId);
            setSelectedRun(fullRun);
            setAiFixes({});
            onMessage?.(`Retry completed with status: ${fullRun.status}`);
            await loadRuns();
        } catch (err) {
            onError?.(err.message || "Retry failed");
        } finally {
            setRetrying(false);
        }
    }

    const handleAiFix = useCallback(async (step) => {
        if (!step.error_message) return;
        const stepId = step.step_id || step.id;
        setAiFixes((prev) => ({
            ...prev,
            [stepId]: { loading: true, result: null, error: "", applied: false },
        }));
        try {
            const data = await fixSqlQuery(
                connectionId,
                databaseName,
                step.executed_sql || selectedPipeline?.steps?.find((s) => s.id === stepId)?.sql_text || "",
                step.error_message,
                {
                    schema: selectedPipeline?.schema_name || "public",
                    object_name: selectedPipeline?.source_object || "",
                }
            );
            setAiFixes((prev) => ({
                ...prev,
                [stepId]: { loading: false, result: data, error: "", applied: false },
            }));
        } catch (err) {
            setAiFixes((prev) => ({
                ...prev,
                [stepId]: { loading: false, result: null, error: err.message || "AI fix failed", applied: false },
            }));
        }
    }, [connectionId, databaseName, selectedPipeline]);

    const handleApplyAiFix = useCallback(async (stepId) => {
        const fix = aiFixes[stepId];
        if (!fix?.result?.sql || !pipelineId) return;
        try {
            const updated = await updatePipelineStep(pipelineId, stepId, {
                sql_text: fix.result.sql,
            });
            setSelectedPipeline?.(updated);
            setAiFixes((prev) => ({
                ...prev,
                [stepId]: { ...prev[stepId], applied: true },
            }));
            onMessage?.("Step SQL updated with AI fix. You can re-run the pipeline.");
        } catch (err) {
            onError?.(err.message || "Failed to apply AI fix");
        }
    }, [aiFixes, pipelineId, setSelectedPipeline, onMessage, onError]);

    if (!pipelineId) {
        return (
            <div className="pipeline-empty">
                Select a pipeline to view run history.
            </div>
        );
    }

    return (
        <div className="pipeline-runs-tab">
            <div className="pipeline-card-header">
                <div>
                    <h3>Run History{pipelineName ? `: ${pipelineName}` : ""}</h3>
                    <p className="pipeline-card-subtitle">
                        Review previous runs and step-by-step results.
                        {livePolling && (
                            <span className="runs-live-indicator" title="Auto-refreshing every 15s">
                                <span className="runs-live-dot" /> Live
                            </span>
                        )}
                    </p>
                </div>
                <div className="pipeline-actions-row">
                    <button
                        className="pipeline-secondary-btn"
                        type="button"
                        onClick={loadRuns}
                        disabled={loading}
                    >
                        {loading ? "Refreshing..." : "Refresh Runs"}
                    </button>
                </div>
            </div>

            <div className="pipeline-split-grid">
                <div className="pipeline-runs-list">
                    {loading && runs.length === 0 ? (
                        <div className="pipeline-empty">Loading runs...</div>
                    ) : runs.length === 0 ? (
                        <div className="pipeline-empty">No runs yet. Execute the pipeline from the Builder tab.</div>
                    ) : (
                        runs.map((run) => (
                            <button
                                key={run.id}
                                type="button"
                                className={`pipeline-run-card ${selectedRun?.id === run.id ? "active" : ""}`}
                                onClick={() => handleOpenRun(run.id)}
                            >
                                <div className="pipeline-run-top">
                                    <span className={`run-status-badge ${statusClass(run.status)}`}>
                                        {run.status}
                                    </span>
                                    <span className="pipeline-run-id">Run #{run.id}</span>
                                </div>
                                <div className="pipeline-run-meta">
                                    <span className="run-trigger-badge">{run.trigger_type || "MANUAL"}</span>
                                    {" · "}Steps: {run.success_steps}/{run.total_steps} success
                                </div>
                                <div className="pipeline-run-meta">
                                    {formatTimestamp(run.started_at)}
                                    {run.duration_seconds != null ? ` · ${formatDuration(run.duration_seconds)}` : ""}
                                </div>
                            </button>
                        ))
                    )}
                </div>

                <div className="pipeline-run-detail-panel">
                    {!selectedRun ? (
                        <div className="pipeline-empty">Select a run to view details.</div>
                    ) : (
                        <div className="pipeline-main-card">
                            <div className="pipeline-card-header">
                                <div>
                                    <h3>Run #{selectedRun.id} Details</h3>
                                    <p className="pipeline-card-subtitle">
                                        {selectedRun.pipeline_name || `Pipeline #${selectedRun.pipeline_id}`}
                                    </p>
                                </div>
                                <div className="pipeline-actions-row">
                                    {(selectedRun.status === "FAILED" || selectedRun.status === "PARTIAL_SUCCESS") && (
                                        <button
                                            className="pipeline-primary-btn"
                                            type="button"
                                            onClick={handleRetryRun}
                                            disabled={retrying}
                                        >
                                            {retrying ? "Retrying..." : "Retry Run"}
                                        </button>
                                    )}
                                </div>
                            </div>

                            <div className="pipeline-run-summary-grid">
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Status</div>
                                    <div className="pipeline-summary-value">
                                        <span className={`run-status-badge ${statusClass(selectedRun.status)}`}>
                                            {selectedRun.status}
                                        </span>
                                    </div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Trigger</div>
                                    <div className="pipeline-summary-value">{selectedRun.trigger_type || "MANUAL"}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Duration</div>
                                    <div className="pipeline-summary-value">{formatDuration(selectedRun.duration_seconds)}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Total Steps</div>
                                    <div className="pipeline-summary-value">{selectedRun.total_steps}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Success</div>
                                    <div className="pipeline-summary-value" style={{ color: "var(--success-text)" }}>{selectedRun.success_steps}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Failed</div>
                                    <div className="pipeline-summary-value" style={{ color: selectedRun.failed_steps > 0 ? "var(--danger-text)" : "inherit" }}>
                                        {selectedRun.failed_steps}
                                    </div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Started</div>
                                    <div className="pipeline-summary-value pipeline-summary-value-sm">{formatTimestamp(selectedRun.started_at)}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Ended</div>
                                    <div className="pipeline-summary-value pipeline-summary-value-sm">{formatTimestamp(selectedRun.ended_at)}</div>
                                </div>
                                {selectedRun.initiated_by ? (
                                    <div className="pipeline-summary-box">
                                        <div className="pipeline-summary-label">Initiated By</div>
                                        <div className="pipeline-summary-value">{selectedRun.initiated_by}</div>
                                    </div>
                                ) : null}
                            </div>

                            {selectedRun.error_message ? (
                                <div className="pipeline-error-box" style={{ marginBottom: "14px" }}>
                                    {selectedRun.error_message}
                                </div>
                            ) : null}

                            <h4 style={{ margin: "14px 0 10px", color: "var(--text)" }}>Step Details</h4>
                            <div className="pipeline-run-steps-list">
                                {(selectedRun.steps || []).map((step) => {
                                    const stepId = step.step_id || step.id;
                                    const fix = aiFixes[stepId];
                                    const isFailed = String(step.status || "").toLowerCase() === "failed";
                                    const stepVerb = getSqlLeadingVerb(step.executed_sql || "");
                                    const stepIsDdl = DDL_KEYWORDS.has(stepVerb);

                                    return (
                                        <div key={step.id} className={`pipeline-step-card-ui ${stepIsDdl ? "pipeline-step-ddl" : ""}`}>
                                            <div className="pipeline-step-header">
                                                <div>
                                                    <div className="pipeline-step-order">Step {step.step_order}</div>
                                                    <div className="pipeline-step-name">{step.step_name}</div>
                                                    {stepIsDdl ? (
                                                        <span className="pipeline-ddl-badge">DDL: {stepVerb.toUpperCase()}</span>
                                                    ) : null}
                                                </div>
                                                <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                                                    <span className={`run-status-badge ${statusClass(step.status)}`}>
                                                        {step.status}
                                                    </span>
                                                    {step.duration_seconds != null && (
                                                        <span className="pipeline-mini-muted">{formatDuration(step.duration_seconds)}</span>
                                                    )}
                                                </div>
                                            </div>

                                            <div className="pipeline-step-meta-row">
                                                <span className="pipeline-mini-muted">
                                                    Rows affected: {step.rows_affected ?? 0}
                                                </span>
                                                {step.started_at && (
                                                    <span className="pipeline-mini-muted">
                                                        Started: {formatTimestamp(step.started_at)}
                                                    </span>
                                                )}
                                            </div>

                                            {step.executed_sql ? (
                                                <details className="pipeline-sql-details">
                                                    <summary>Executed SQL</summary>
                                                    <pre className={`pipeline-code-block ${stepIsDdl ? "pipeline-code-ddl" : ""}`}>{step.executed_sql}</pre>
                                                </details>
                                            ) : null}

                                            {step.error_message ? (
                                                <div className="pipeline-error-box" style={{ marginTop: "10px" }}>
                                                    {step.error_message}
                                                </div>
                                            ) : null}

                                            {isFailed && step.error_message ? (
                                                <div style={{ marginTop: "10px" }}>
                                                    {!fix ? (
                                                        <button
                                                            className="pipeline-primary-btn"
                                                            type="button"
                                                            onClick={() => handleAiFix(step)}
                                                        >
                                                            AI Fix SQL
                                                        </button>
                                                    ) : fix.loading ? (
                                                        <div className="pipeline-mini-muted">Generating AI fix...</div>
                                                    ) : fix.error ? (
                                                        <div className="pipeline-error-box">{fix.error}</div>
                                                    ) : fix.result ? (
                                                        <div className="pipeline-ai-fix-panel">
                                                            <div style={{ fontWeight: 600, marginBottom: "6px", color: "var(--success-text)" }}>
                                                                AI Suggested Fix
                                                            </div>
                                                            {fix.result.explanation && (
                                                                <div className="pipeline-mini-muted" style={{ marginBottom: "8px" }}>
                                                                    {fix.result.explanation}
                                                                </div>
                                                            )}
                                                            <pre className="pipeline-code-block" style={{ maxHeight: "200px", overflow: "auto", fontSize: "12px", marginBottom: "8px" }}>
                                                                {fix.result.sql}
                                                            </pre>
                                                            {fix.result.assumptions?.length ? (
                                                                <div className="pipeline-mini-muted" style={{ marginBottom: "4px" }}>
                                                                    <strong>Assumptions:</strong> {fix.result.assumptions.join("; ")}
                                                                </div>
                                                            ) : null}
                                                            {fix.result.warnings?.length ? (
                                                                <div className="pipeline-mini-muted" style={{ marginBottom: "8px", color: "#b35900" }}>
                                                                    <strong>Warnings:</strong> {fix.result.warnings.join("; ")}
                                                                </div>
                                                            ) : null}
                                                            <div style={{ display: "flex", gap: "8px" }}>
                                                                {!fix.applied ? (
                                                                    <button className="pipeline-primary-btn" type="button" onClick={() => handleApplyAiFix(stepId)}>
                                                                        Apply Fix to Step
                                                                    </button>
                                                                ) : (
                                                                    <span style={{ color: "var(--success-text)", fontWeight: 600 }}>
                                                                        Fix applied to pipeline definition — re-run to test. This past run&apos;s SQL is unchanged.
                                                                    </span>
                                                                )}
                                                                <button className="pipeline-secondary-btn" type="button" onClick={() => handleAiFix(step)}>
                                                                    Retry Fix
                                                                </button>
                                                            </div>
                                                        </div>
                                                    ) : null}
                                                </div>
                                            ) : null}

                                            {step.step_log && !isFailed ? (
                                                <div className="pipeline-log-box" style={{ marginTop: "10px" }}>
                                                    {step.step_log}
                                                </div>
                                            ) : null}
                                        </div>
                                    );
                                })}
                            </div>

                            {selectedRun.run_log ? (
                                <details className="pipeline-sql-details" style={{ marginTop: "14px" }}>
                                    <summary>Full Run Log</summary>
                                    <pre className="pipeline-code-block">{selectedRun.run_log}</pre>
                                </details>
                            ) : null}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
