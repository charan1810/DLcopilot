import { useEffect, useMemo, useState, useCallback } from "react";
import {
    fetchPipelines,
    createPipeline,
    getPipeline,
    updatePipeline,
    deletePipeline,
    addPipelineStep,
    importPipelineSteps,
    removePipelineStep,
    updatePipelineStep,
    executePipeline,
    fetchPipelineRuns,
    fetchPipelineRun,
    fixSqlQuery,
    agenticGeneratePipelineSteps,
} from "../api/schemaApi";
import PipelineRunsTab from "./PipelineRunsTab";
import PipelineScheduleTab from "./PipelineScheduleTab";

function buildDefaultPipelineForm(connectionId, databaseName, schemaName, selectedObject) {
    return {
        name: "",
        description: "",
        connection_id: connectionId || "",
        database_name: databaseName || "",
        schema_name: schemaName || "",
        source_object: selectedObject || "",
        target_object: "",
    };
}

function buildDefaultStepForm() {
    return {
        step_name: "",
        sql_text: "",
    };
}

function normalizePipelineSession(session, connectionId, databaseName, schemaName, selectedObject) {
    return {
        pipelines: Array.isArray(session?.pipelines) ? session.pipelines : [],
        selectedPipelineId: session?.selectedPipelineId ?? null,
        selectedPipeline: session?.selectedPipeline ?? null,
        runs: Array.isArray(session?.runs) ? session.runs : [],
        selectedRun: session?.selectedRun ?? null,
        message: session?.message || "",
        error: session?.error || "",
        pipelineForm: session?.pipelineForm || buildDefaultPipelineForm(connectionId, databaseName, schemaName, selectedObject),
        stepForm: session?.stepForm || buildDefaultStepForm(),
    };
}

function statusClass(status) {
    const key = String(status || "").toLowerCase();
    if (key === "success") return "run-status-success";
    if (key === "failed") return "run-status-failed";
    if (key === "running") return "run-status-running";
    if (key === "partial_success") return "run-status-partial_success";
    return "run-status-running";
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

function isDdlSql(sqlText = "") {
    return DDL_KEYWORDS.has(getSqlLeadingVerb(sqlText));
}

export default function PipelineBuilder({
    sessionKey,
    connectionId,
    databaseName,
    schemaName,
    selectedObject,
    currentTransformationSql = "",
    currentTransformationSource = null,
    pipelineSession,
    updatePipelineSession,
}) {
    const [pipelines, setPipelines] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).pipelines);
    const [selectedPipelineId, setSelectedPipelineId] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).selectedPipelineId);
    const [selectedPipeline, setSelectedPipeline] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).selectedPipeline);
    const [runs, setRuns] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).runs);
    const [selectedRun, setSelectedRun] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).selectedRun);

    const [loadingList, setLoadingList] = useState(false);
    const [savingPipeline, setSavingPipeline] = useState(false);
    const [executing, setExecuting] = useState(false);
    const [activeSubtab, setActiveSubtab] = useState("builder");
    const [message, setMessage] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).message);
    const [error, setError] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).error);

    const [pipelineForm, setPipelineForm] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).pipelineForm);

    const [stepForm, setStepForm] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).stepForm);

    const canCreatePipeline = useMemo(() => {
        return !!pipelineForm.name?.trim() && !!pipelineForm.connection_id;
    }, [pipelineForm]);

    useEffect(() => {
        const next = normalizePipelineSession(
            pipelineSession,
            connectionId,
            databaseName,
            schemaName,
            selectedObject
        );

        setPipelines(next.pipelines);
        setSelectedPipelineId(next.selectedPipelineId);
        setSelectedPipeline(next.selectedPipeline);
        setRuns(next.runs);
        setSelectedRun(next.selectedRun);
        setMessage(next.message);
        setError(next.error);
        setPipelineForm(next.pipelineForm);
        setStepForm(next.stepForm);
    }, [sessionKey]);

    useEffect(() => {
        setPipelineForm((prev) => ({
            ...prev,
            connection_id: connectionId || prev.connection_id || "",
            database_name: databaseName || prev.database_name || "",
            schema_name: schemaName || prev.schema_name || "",
            source_object: selectedObject || prev.source_object || "",
        }));
    }, [connectionId, databaseName, schemaName, selectedObject]);

    useEffect(() => {
        if (!updatePipelineSession) return;

        updatePipelineSession({
            pipelines,
            selectedPipelineId,
            selectedPipeline,
            runs,
            selectedRun,
            message,
            error,
            pipelineForm,
            stepForm,
        });
    }, [
        pipelines,
        selectedPipelineId,
        selectedPipeline,
        runs,
        selectedRun,
        message,
        error,
        pipelineForm,
        stepForm,
        updatePipelineSession,
    ]);

    useEffect(() => {
        if (!connectionId) return;
        loadPipelines();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [connectionId, databaseName, schemaName]);

    async function loadPipelines() {
        try {
            setLoadingList(true);
            setError("");
            const data = await fetchPipelines({
                database_name: databaseName,
                schema_name: schemaName,
            });
            setPipelines(Array.isArray(data) ? data : []);
        } catch (err) {
            setError(err.message || "Failed to load pipelines");
            setPipelines([]);
        } finally {
            setLoadingList(false);
        }
    }

    async function loadPipelineDetails(pipelineId) {
        try {
            setError("");
            setMessage("");

            const data = await getPipeline(pipelineId);
            setSelectedPipeline(data);
            setSelectedPipelineId(data.id);

            setPipelineForm({
                name: data.name || "",
                description: data.description || "",
                connection_id: data.connection_id || "",
                database_name: data.database_name || "",
                schema_name: data.schema_name || "",
                source_object: data.source_object || "",
                target_object: data.target_object || "",
            });

            const runList = await fetchPipelineRuns(pipelineId);
            setRuns(Array.isArray(runList) ? runList : []);
            setSelectedRun(null);
        } catch (err) {
            setError(err.message || "Failed to load pipeline details");
        }
    }

    function resetSelection() {
        setSelectedPipelineId(null);
        setSelectedPipeline(null);
        setSelectedRun(null);
        setRuns([]);
        setPipelineForm(buildDefaultPipelineForm(connectionId, databaseName, schemaName, selectedObject));
        setStepForm(buildDefaultStepForm());
        setMessage("");
        setError("");
    }

    async function handleCreatePipeline() {
        try {
            setSavingPipeline(true);
            setError("");
            setMessage("");

            const created = await createPipeline({
                ...pipelineForm,
                connection_id: Number(pipelineForm.connection_id),
            });

            setMessage("Pipeline created successfully.");
            await loadPipelines();
            await loadPipelineDetails(created.id);
        } catch (err) {
            setError(err.message || "Failed to create pipeline");
        } finally {
            setSavingPipeline(false);
        }
    }

    async function handleSavePipeline() {
        if (!selectedPipelineId) return;

        try {
            setSavingPipeline(true);
            setError("");
            setMessage("");

            await updatePipeline(selectedPipelineId, {
                ...pipelineForm,
                connection_id: Number(pipelineForm.connection_id),
            });

            setMessage("Pipeline updated successfully.");
            await loadPipelines();
            await loadPipelineDetails(selectedPipelineId);
        } catch (err) {
            setError(err.message || "Failed to update pipeline");
        } finally {
            setSavingPipeline(false);
        }
    }

    async function handleDeletePipeline() {
        if (!selectedPipelineId) return;
        const ok = window.confirm("Delete this pipeline?");
        if (!ok) return;

        try {
            setError("");
            setMessage("");

            await deletePipeline(selectedPipelineId);
            setMessage("Pipeline deleted successfully.");
            resetSelection();
            await loadPipelines();
        } catch (err) {
            setError(err.message || "Failed to delete pipeline");
        }
    }

    async function handleAddStep() {
        if (!selectedPipelineId) {
            setError("Please create or select a pipeline first.");
            return;
        }

        if (!stepForm.step_name.trim() || !stepForm.sql_text.trim()) {
            setError("Step name and SQL are required.");
            return;
        }

        try {
            setError("");
            setMessage("");

            const updated = await addPipelineStep(selectedPipelineId, {
                step_name: stepForm.step_name,
                sql_text: stepForm.sql_text,
                step_type: "sql",
                is_active: 1,
            });

            setSelectedPipeline(updated);
            setStepForm({
                step_name: "",
                sql_text: "",
            });

            setMessage("Pipeline step added.");
        } catch (err) {
            setError(err.message || "Failed to add step");
        }
    }

    async function handleImportCurrentTransformation() {
        if (!selectedPipelineId) {
            setError("Please create or select a pipeline first.");
            return;
        }

        if (!currentTransformationSql?.trim()) {
            setError("No current transformation SQL available to import.");
            return;
        }

        if (selectedPipeline?.steps?.length) {
            const shouldReplace = window.confirm(
                "Importing the current transformation will replace the existing pipeline steps. Do you want to continue?"
            );
            if (!shouldReplace) {
                return;
            }
        }

        try {
            setError("");
            setMessage("");

            const updated = await importCurrentTransformationToPipeline(selectedPipelineId);

            setSelectedPipeline(updated);
            setMessage(
                currentTransformationSource?.objectName
                    ? `Imported transformation from ${currentTransformationSource.objectName} into pipeline.`
                    : "Current transformation imported into pipeline."
            );
        } catch (err) {
            setError(err.message || "Failed to import transformation");
        }
    }

    async function importCurrentTransformationToPipeline(pipelineId) {
        return importPipelineSteps(pipelineId, {
            steps: [
                {
                    step_name:
                        currentTransformationSource?.title ||
                        `Transformation from ${currentTransformationSource?.objectName || "current session"}`,
                    step_type: "sql",
                    sql_text: currentTransformationSql,
                    is_active: 1,
                },
            ],
        });
    }

    async function replacePipelineWithManualStep(pipelineId) {
        return importPipelineSteps(pipelineId, {
            steps: [
                {
                    step_name: stepForm.step_name,
                    step_type: "sql",
                    sql_text: stepForm.sql_text,
                    is_active: 1,
                },
            ],
        });
    }

    async function handleDeleteStep(stepId) {
        if (!selectedPipelineId) return;

        try {
            setError("");
            setMessage("");

            const updated = await removePipelineStep(selectedPipelineId, stepId);
            setSelectedPipeline(updated);
            setMessage("Step deleted successfully.");
        } catch (err) {
            setError(err.message || "Failed to delete step");
        }
    }

    async function handleExecutePipeline() {
        if (!selectedPipelineId) {
            setError("Please select a pipeline first.");
            return;
        }

        try {
            setExecuting(true);
            setError("");
            setMessage("");

            let activePipeline = selectedPipeline;
            let runSourceLabel = "pipeline";
            const activeSteps = (activePipeline?.steps || []).filter(
                (step) => Number(step.is_active ?? 1) === 1
            );
            const hasDraftStep = !!stepForm.step_name.trim() && !!stepForm.sql_text.trim();

            if (hasDraftStep) {
                const shouldUseDraftStep = window.confirm(
                    activeSteps.length
                        ? "A manual step is currently in the editor. Replace the saved pipeline steps with this manual step and run it now?"
                        : "A manual step is currently in the editor. Save this step to the pipeline and run it now?"
                );

                if (!shouldUseDraftStep) {
                    setMessage("Pipeline execution cancelled. Save the current step or clear the editor draft before running saved pipeline steps.");
                    return;
                }

                activePipeline = activeSteps.length
                    ? await replacePipelineWithManualStep(selectedPipelineId)
                    : await addPipelineStep(selectedPipelineId, {
                        step_name: stepForm.step_name,
                        sql_text: stepForm.sql_text,
                        step_type: "sql",
                        is_active: 1,
                    });

                setSelectedPipeline(activePipeline);
                setStepForm(buildDefaultStepForm());
                runSourceLabel = "manual";
            } else if (!activeSteps.length) {
                setError("No active steps found in pipeline. Add a manual step or use Import Current Transformation explicitly before running.");
                return;
            }

            const runnableSteps = (activePipeline?.steps || []).filter(
                (step) => Number(step.is_active ?? 1) === 1
            );
            const ddlSteps = runnableSteps.filter((step) => isDdlSql(step.sql_text || ""));
            let allowDdlExecute = false;

            if (ddlSteps.length) {
                const stepList = ddlSteps
                    .map((step) => {
                        const verb = (getSqlLeadingVerb(step.sql_text || "") || "ddl").toUpperCase();
                        return `- Step ${step.step_order}: ${step.step_name} [${verb}]`;
                    })
                    .join("\n");

                const approved = window.confirm(
                    `This run contains ${ddlSteps.length} DDL statement(s) (CREATE/ALTER/DROP/TRUNCATE).\n\n${stepList}\n\nDDL is blocked by default. Do you want to approve and run these statements now?`
                );

                if (!approved) {
                    setMessage("Pipeline execution cancelled. DDL steps need explicit approval.");
                    return;
                }

                allowDdlExecute = true;
            }

            const run = await executePipeline(selectedPipelineId, {
                stop_on_error: true,
                allow_ddl_execute: allowDdlExecute,
            });

            setSelectedRun(run);
            setMessage(
                runSourceLabel === "manual"
                    ? `Manual step added and pipeline execution finished with status: ${run.status}`
                    : `Pipeline execution finished with status: ${run.status}`
            );

            const updatedRuns = await fetchPipelineRuns(selectedPipelineId);
            setRuns(Array.isArray(updatedRuns) ? updatedRuns : []);
        } catch (err) {
            setError(err.message || "Pipeline execution failed");
        } finally {
            setExecuting(false);
        }
    }

    async function handleOpenRun(runId) {
        try {
            setError("");
            setMessage("");
            const run = await fetchPipelineRun(runId);
            setSelectedRun(run);
            setAiFixes({});
        } catch (err) {
            setError(err.message || "Failed to load run details");
        }
    }

    // --- AI Fix for failed steps ---
    const [aiFixes, setAiFixes] = useState({});
    // aiFixes: { [stepId]: { loading, result, error, applied } }

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
                // Get the original SQL from the pipeline step
                selectedPipeline?.steps?.find((s) => s.id === stepId)?.sql_text || "",
                step.error_message,
                {
                    schema: selectedPipeline?.schema_name || schemaName || "public",
                    object_name: selectedPipeline?.source_object || selectedObject || "",
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
    }, [connectionId, databaseName, selectedPipeline, schemaName, selectedObject]);

    const handleApplyAiFix = useCallback(async (stepId) => {
        const fix = aiFixes[stepId];
        if (!fix?.result?.sql || !selectedPipelineId) return;

        try {
            const updated = await updatePipelineStep(selectedPipelineId, stepId, {
                sql_text: fix.result.sql,
            });
            setSelectedPipeline(updated);
            setAiFixes((prev) => ({
                ...prev,
                [stepId]: { ...prev[stepId], applied: true },
            }));
            setMessage(`Step SQL updated with AI fix. You can re-run the pipeline.`);
        } catch (err) {
            setError(err.message || "Failed to apply AI fix to step");
        }
    }, [aiFixes, selectedPipelineId]);

    // --- Agentic AI Step Generator ---
    const [showAgentPanel, setShowAgentPanel] = useState(false);
    const [agentRequirement, setAgentRequirement] = useState("");
    const [agentGenerating, setAgentGenerating] = useState(false);
    const [agentResult, setAgentResult] = useState(null);
    const [agentError, setAgentError] = useState("");
    const [agentExpandedSteps, setAgentExpandedSteps] = useState({});
    const [agentAddingAll, setAgentAddingAll] = useState(false);
    const [agentAddedSteps, setAgentAddedSteps] = useState(new Set());

    const handleAgentGenerate = useCallback(async () => {
        if (!selectedPipelineId) {
            setAgentError("Select or create a pipeline first.");
            return;
        }
        if (!agentRequirement.trim()) {
            setAgentError("Please describe what this pipeline should do.");
            return;
        }

        setAgentGenerating(true);
        setAgentError("");
        setAgentResult(null);
        setAgentAddedSteps(new Set());
        setAgentExpandedSteps({});

        try {
            const result = await agenticGeneratePipelineSteps(
                selectedPipelineId,
                agentRequirement.trim()
            );
            setAgentResult(result);
        } catch (err) {
            setAgentError(err.message || "Agentic generation failed");
        } finally {
            setAgentGenerating(false);
        }
    }, [selectedPipelineId, agentRequirement]);

    const handleAgentAddStep = useCallback(async (step, idx) => {
        if (!selectedPipelineId) return;
        try {
            const updated = await addPipelineStep(selectedPipelineId, {
                step_name: step.step_name,
                sql_text: step.sql_text,
                step_type: "sql",
                is_active: 1,
            });
            setSelectedPipeline(updated);
            setAgentAddedSteps((prev) => new Set(prev).add(idx));
            setMessage(`Step "${step.step_name}" added to pipeline.`);
        } catch (err) {
            setError(err.message || "Failed to add step");
        }
    }, [selectedPipelineId]);

    const handleAgentAddAllSteps = useCallback(async () => {
        if (!selectedPipelineId || !agentResult?.steps?.length) return;
        setAgentAddingAll(true);
        setError("");
        setMessage("");
        const newAdded = new Set(agentAddedSteps);
        let lastPipeline = selectedPipeline;
        let addedCount = 0;
        for (let i = 0; i < agentResult.steps.length; i++) {
            if (newAdded.has(i)) continue;
            const step = agentResult.steps[i];
            try {
                lastPipeline = await addPipelineStep(selectedPipelineId, {
                    step_name: step.step_name,
                    sql_text: step.sql_text,
                    step_type: "sql",
                    is_active: 1,
                });
                newAdded.add(i);
                addedCount++;
            } catch (err) {
                setError(`Failed on step "${step.step_name}": ${err.message}`);
                break;
            }
        }
        setSelectedPipeline(lastPipeline);
        setAgentAddedSteps(newAdded);
        setAgentAddingAll(false);
        if (addedCount > 0) {
            setMessage(`Added ${addedCount} AI-generated step(s) to pipeline.`);
        }
    }, [selectedPipelineId, agentResult, agentAddedSteps, selectedPipeline]);

    return (
        <div className="pipeline-builder-shell">
            <div className="pipeline-builder-grid">
                <div className="pipeline-sidebar-card">
                    <div className="pipeline-sidebar-header">
                        <h2 className="pipeline-sidebar-title">Pipeline Builder</h2>
                        <button className="pipeline-primary-btn" type="button" onClick={resetSelection}>
                            New
                        </button>
                    </div>

                    <div className="pipeline-context-box">
                        <div className="pipeline-context-item">
                            <span>Connection:</span>{connectionId || "-"}
                        </div>
                        <div className="pipeline-context-item">
                            <span>Database:</span>{databaseName || "-"}
                        </div>
                        <div className="pipeline-context-item">
                            <span>Schema:</span>{schemaName || "-"}
                        </div>
                        <div className="pipeline-context-item">
                            <span>Object:</span>{selectedObject || "-"}
                        </div>
                    </div>

                    <div className="pipeline-list-scroll">
                        {loadingList ? (
                            <div className="pipeline-empty">Loading pipelines...</div>
                        ) : pipelines.length === 0 ? (
                            <div className="pipeline-empty">No pipelines found.</div>
                        ) : (
                            pipelines.map((item) => (
                                <button
                                    key={item.id}
                                    type="button"
                                    className={`pipeline-list-item ${selectedPipelineId === item.id ? "active" : ""}`}
                                    onClick={() => loadPipelineDetails(item.id)}
                                >
                                    <div className="pipeline-list-name">{item.name}</div>
                                    <div className="pipeline-list-meta">
                                        {item.schema_name || "-"} / {item.source_object || "-"}
                                    </div>
                                </button>
                            ))
                        )}
                    </div>
                </div>

                <div className="pipeline-main-stack">
                    {(message || error) && (
                        <div className={`pipeline-inline-banner ${error ? "error" : "success"}`}>
                            {error || message}
                        </div>
                    )}

                    {/* Subtab bar */}
                    <div className="pipeline-subtab-bar">
                        <button
                            type="button"
                            className={`pipeline-subtab-btn ${activeSubtab === "builder" ? "pipeline-subtab-btn-active" : ""}`}
                            onClick={() => setActiveSubtab("builder")}
                        >
                            Builder
                        </button>
                        <button
                            type="button"
                            className={`pipeline-subtab-btn ${activeSubtab === "runs" ? "pipeline-subtab-btn-active" : ""}`}
                            onClick={() => setActiveSubtab("runs")}
                        >
                            Runs
                        </button>
                        <button
                            type="button"
                            className={`pipeline-subtab-btn ${activeSubtab === "schedule" ? "pipeline-subtab-btn-active" : ""}`}
                            onClick={() => setActiveSubtab("schedule")}
                        >
                            Schedule
                        </button>
                    </div>

                    {/* ========== BUILDER SUBTAB ========== */}
                    {activeSubtab === "builder" && (
                        <>
                            <div className="pipeline-main-card">
                                <div className="pipeline-card-header">
                                    <div>
                                        <h3>{selectedPipelineId ? "Edit Pipeline" : "Create Pipeline"}</h3>
                                        <p className="pipeline-card-subtitle">
                                            Define the pipeline header and target metadata.
                                        </p>
                                    </div>

                                    <div className="pipeline-actions-row">
                                        {!selectedPipelineId ? (
                                            <button
                                                className="pipeline-primary-btn"
                                                type="button"
                                                disabled={!canCreatePipeline || savingPipeline}
                                                onClick={handleCreatePipeline}
                                            >
                                                {savingPipeline ? "Creating..." : "Create Pipeline"}
                                            </button>
                                        ) : (
                                            <>
                                                <button
                                                    className="pipeline-primary-btn"
                                                    type="button"
                                                    disabled={savingPipeline}
                                                    onClick={handleSavePipeline}
                                                >
                                                    {savingPipeline ? "Saving..." : "Save Pipeline"}
                                                </button>
                                                <button
                                                    className="pipeline-danger-btn"
                                                    type="button"
                                                    onClick={handleDeletePipeline}
                                                >
                                                    Delete
                                                </button>
                                            </>
                                        )}
                                    </div>
                                </div>

                                <div className="pipeline-form-grid">
                                    <div className="form-field">
                                        <label>Pipeline Name</label>
                                        <input
                                            type="text"
                                            value={pipelineForm.name}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, name: e.target.value }))
                                            }
                                            placeholder="e.g. Customer Standardization Pipeline"
                                        />
                                    </div>

                                    <div className="form-field">
                                        <label>Target Object</label>
                                        <input
                                            type="text"
                                            value={pipelineForm.target_object}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, target_object: e.target.value }))
                                            }
                                            placeholder="e.g. customers_clean"
                                        />
                                    </div>

                                    <div className="form-field">
                                        <label>Database</label>
                                        <input
                                            type="text"
                                            value={pipelineForm.database_name}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, database_name: e.target.value }))
                                            }
                                            placeholder="Database name"
                                        />
                                    </div>

                                    <div className="form-field">
                                        <label>Schema</label>
                                        <input
                                            type="text"
                                            value={pipelineForm.schema_name}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, schema_name: e.target.value }))
                                            }
                                            placeholder="Schema name"
                                        />
                                    </div>

                                    <div className="form-field">
                                        <label>Source Object</label>
                                        <input
                                            type="text"
                                            value={pipelineForm.source_object}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, source_object: e.target.value }))
                                            }
                                            placeholder="e.g. customers_src"
                                        />
                                    </div>

                                    <div className="form-field">
                                        <label>Connection ID</label>
                                        <input
                                            type="number"
                                            value={pipelineForm.connection_id}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, connection_id: e.target.value }))
                                            }
                                            placeholder="Connection ID"
                                        />
                                    </div>

                                    <div className="form-field form-field-full">
                                        <label>Description</label>
                                        <textarea
                                            rows="4"
                                            value={pipelineForm.description}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, description: e.target.value }))
                                            }
                                            placeholder="Pipeline description"
                                        />
                                    </div>
                                </div>
                            </div>

                            <div className="pipeline-main-card">
                                <div className="pipeline-card-header">
                                    <div>
                                        <h3>Pipeline Steps</h3>
                                        <p className="pipeline-card-subtitle">
                                            Build the ordered SQL steps for this pipeline.
                                        </p>
                                    </div>

                                    <div className="pipeline-actions-row">
                                        <button
                                            className="pipeline-secondary-btn"
                                            type="button"
                                            onClick={handleImportCurrentTransformation}
                                            disabled={!selectedPipelineId || !currentTransformationSql?.trim()}
                                        >
                                            Import Current Transformation
                                        </button>
                                        <button
                                            className="pipeline-secondary-btn"
                                            type="button"
                                            onClick={() => {
                                                setShowAgentPanel((v) => !v);
                                                setAgentError("");
                                            }}
                                            disabled={!selectedPipelineId}
                                            title="Use Agentic AI to automatically plan, generate, and validate all pipeline SQL steps from your requirements"
                                        >
                                            {showAgentPanel ? "Hide AI Agent" : "✦ AI Generate Steps"}
                                        </button>
                                        <button
                                            className="pipeline-primary-btn"
                                            type="button"
                                            onClick={handleExecutePipeline}
                                            disabled={!selectedPipelineId || executing}
                                        >
                                            {executing ? "Executing..." : "Run Pipeline"}
                                        </button>
                                    </div>
                                </div>

                                <div className="pipeline-mini-muted" style={{ marginBottom: "12px" }}>
                                    Transformations source: {currentTransformationSource?.databaseName || "-"}
                                    {currentTransformationSource?.schemaName ? ` / ${currentTransformationSource.schemaName}` : ""}
                                    {currentTransformationSource?.objectName ? ` / ${currentTransformationSource.objectName}` : ""}
                                    {!currentTransformationSql?.trim() ? " | No generated SQL available in Transformations tab." : ""}
                                </div>

                                {/* ========== AGENTIC AI PANEL ========== */}
                                {showAgentPanel && (
                                    <div className="agent-panel">
                                        <div className="agent-panel-header">
                                            <span className="agent-panel-title">✦ Agentic AI Step Generator</span>
                                            <span className="agent-panel-subtitle">
                                                Reads your real database metadata → plans steps → generates validated, copy-paste SQL
                                            </span>
                                        </div>

                                        <div className="agent-phase-badges">
                                            <span className="agent-phase-badge">① Fetch Metadata</span>
                                            <span className="agent-phase-arrow">→</span>
                                            <span className="agent-phase-badge">② Plan Steps</span>
                                            <span className="agent-phase-arrow">→</span>
                                            <span className="agent-phase-badge">③ Generate SQL</span>
                                            <span className="agent-phase-arrow">→</span>
                                            <span className="agent-phase-badge">④ Validate &amp; Self-Heal</span>
                                        </div>

                                        <div className="form-field" style={{ marginTop: "12px" }}>
                                            <label>What should this pipeline do?</label>
                                            <textarea
                                                rows={4}
                                                value={agentRequirement}
                                                onChange={(e) => setAgentRequirement(e.target.value)}
                                                placeholder={
                                                    "Describe the full pipeline in plain English.\n" +
                                                    "Example: Load customers_src into a staging table, deduplicate by customer_id keeping the latest record, then merge into customers_clean updating changed rows and inserting new ones."
                                                }
                                                disabled={agentGenerating}
                                            />
                                        </div>

                                        <div className="agent-panel-toolbar">
                                            <button
                                                className="pipeline-primary-btn agent-generate-btn"
                                                type="button"
                                                disabled={agentGenerating || !agentRequirement.trim()}
                                                onClick={handleAgentGenerate}
                                            >
                                                {agentGenerating ? (
                                                    <span className="agent-spinner-row">
                                                        <span className="agent-spinner" />
                                                        Agent running…
                                                    </span>
                                                ) : "Generate Pipeline Steps"}
                                            </button>
                                            {agentResult && (
                                                <button
                                                    className="pipeline-secondary-btn"
                                                    type="button"
                                                    onClick={() => { setAgentResult(null); setAgentRequirement(""); setAgentAddedSteps(new Set()); }}
                                                >
                                                    Clear Results
                                                </button>
                                            )}
                                        </div>

                                        {agentError && (
                                            <div className="agent-error-box">{agentError}</div>
                                        )}

                                        {agentGenerating && (
                                            <div className="agent-progress-box">
                                                <div className="agent-progress-row">
                                                    <span className="agent-spinner" />
                                                    <span>Agent is reading your database schema, planning steps, generating and validating SQL…</span>
                                                </div>
                                                <div className="agent-progress-phases">
                                                    <div className="agent-progress-phase active">① Fetching real column metadata from database</div>
                                                    <div className="agent-progress-phase">② Planning ordered pipeline steps</div>
                                                    <div className="agent-progress-phase">③ Generating per-step SQL with full schema context</div>
                                                    <div className="agent-progress-phase">④ Validating column references &amp; self-healing errors</div>
                                                </div>
                                            </div>
                                        )}

                                        {agentResult && (
                                            <div className="agent-results">
                                                <div className="agent-results-header">
                                                    <div className="agent-results-summary">
                                                        <span className="agent-badge-total">{agentResult.total_steps} steps planned</span>
                                                        <span className="agent-badge-validated">{agentResult.validated_steps} validated</span>
                                                        {agentResult.plan_summary && (
                                                            <span className="agent-plan-summary">{agentResult.plan_summary}</span>
                                                        )}
                                                    </div>
                                                    <button
                                                        className="pipeline-primary-btn"
                                                        type="button"
                                                        disabled={agentAddingAll || agentResult.steps.length === agentAddedSteps.size}
                                                        onClick={handleAgentAddAllSteps}
                                                    >
                                                        {agentAddingAll ? "Adding…" : `Add All ${agentResult.steps.length} Steps to Pipeline`}
                                                    </button>
                                                </div>

                                                {agentResult.steps.map((step, idx) => {
                                                    const aiStepVerb = getSqlLeadingVerb(step.sql_text || "");
                                                    const aiStepIsDdl = DDL_KEYWORDS.has(aiStepVerb);

                                                    return (
                                                        <div key={idx} className={`agent-step-card ${step.validated ? "agent-step-validated" : "agent-step-partial"}`}>
                                                            <div className="agent-step-header">
                                                                <div className="agent-step-meta">
                                                                    <span className="agent-step-order">Step {idx + 1}</span>
                                                                    <span className="agent-step-name">{step.step_name}</span>
                                                                    {step.validated
                                                                        ? <span className="agent-badge-ok">✓ Validated</span>
                                                                        : <span className="agent-badge-warn">⚠ Review</span>
                                                                    }
                                                                    {aiStepIsDdl ? (
                                                                        <span className="agent-badge-ddl">DDL: {aiStepVerb.toUpperCase()}</span>
                                                                    ) : null}
                                                                    {agentAddedSteps.has(idx) && (
                                                                        <span className="agent-badge-added">✓ Added</span>
                                                                    )}
                                                                </div>
                                                                <div className="agent-step-actions">
                                                                    <button
                                                                        className="agent-toggle-btn"
                                                                        type="button"
                                                                        onClick={() => setAgentExpandedSteps((prev) => ({ ...prev, [idx]: !prev[idx] }))}
                                                                    >
                                                                        {agentExpandedSteps[idx] ? "Hide SQL" : "Show SQL"}
                                                                    </button>
                                                                    <button
                                                                        className="pipeline-primary-btn agent-add-btn"
                                                                        type="button"
                                                                        disabled={agentAddedSteps.has(idx)}
                                                                        onClick={() => handleAgentAddStep(step, idx)}
                                                                    >
                                                                        {agentAddedSteps.has(idx) ? "Added" : "Add Step"}
                                                                    </button>
                                                                </div>
                                                            </div>

                                                            {step.explanation && (
                                                                <div className="agent-step-explanation">{step.explanation}</div>
                                                            )}

                                                            {agentExpandedSteps[idx] && (
                                                                <pre className="agent-sql-block">{step.sql_text}</pre>
                                                            )}

                                                            {step.warnings?.length > 0 && (
                                                                <div className="agent-step-warnings">
                                                                    {step.warnings.map((w, wi) => (
                                                                        <div key={wi} className="agent-warning-item">⚠ {w}</div>
                                                                    ))}
                                                                </div>
                                                            )}

                                                            {step.assumptions?.length > 0 && (
                                                                <div className="agent-step-assumptions">
                                                                    {step.assumptions.map((a, ai) => (
                                                                        <div key={ai} className="agent-assumption-item">ℹ {a}</div>
                                                                    ))}
                                                                </div>
                                                            )}
                                                        </div>
                                                    );
                                                })}

                                                {/* Agent log accordion */}
                                                <details className="agent-log-details">
                                                    <summary className="agent-log-summary">Agent Execution Log ({agentResult.agent_log?.length || 0} events)</summary>
                                                    <div className="agent-log-body">
                                                        {(agentResult.agent_log || []).map((entry, ei) => (
                                                            <div key={ei} className={`agent-log-entry agent-log-${entry.status}`}>
                                                                <span className="agent-log-phase">[{entry.phase}]</span>
                                                                {entry.step && <span className="agent-log-step"> {entry.step}:</span>}
                                                                <span className="agent-log-detail"> {entry.details}</span>
                                                            </div>
                                                        ))}
                                                    </div>
                                                </details>
                                            </div>
                                        )}
                                    </div>
                                )}

                                <div className="pipeline-step-editor">
                                    <div className="form-field">
                                        <label>Step Name</label>
                                        <input
                                            type="text"
                                            value={stepForm.step_name}
                                            onChange={(e) =>
                                                setStepForm((prev) => ({ ...prev, step_name: e.target.value }))
                                            }
                                            placeholder="e.g. Create staging table"
                                        />
                                    </div>

                                    <div className="form-field">
                                        <label>SQL</label>
                                        <textarea
                                            rows="8"
                                            value={stepForm.sql_text}
                                            onChange={(e) =>
                                                setStepForm((prev) => ({ ...prev, sql_text: e.target.value }))
                                            }
                                            placeholder="Paste SQL step here..."
                                        />
                                    </div>

                                    <div className="pipeline-step-toolbar">
                                        <button
                                            className="pipeline-primary-btn"
                                            type="button"
                                            onClick={handleAddStep}
                                            disabled={!selectedPipelineId}
                                        >
                                            Add Step
                                        </button>
                                    </div>
                                </div>

                                <div className="pipeline-steps-list">
                                    {!selectedPipeline?.steps?.length ? (
                                        <div className="pipeline-empty">No pipeline steps added yet.</div>
                                    ) : (
                                        selectedPipeline.steps.map((step) => {
                                            const stepVerb = getSqlLeadingVerb(step.sql_text || "");
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
                                                        <button
                                                            className="pipeline-danger-btn"
                                                            type="button"
                                                            onClick={() => handleDeleteStep(step.id)}
                                                        >
                                                            Remove
                                                        </button>
                                                    </div>

                                                    <pre className={`pipeline-code-block ${stepIsDdl ? "pipeline-code-ddl" : ""}`}>{step.sql_text}</pre>
                                                </div>
                                            );
                                        })
                                    )}
                                </div>
                            </div>

                            {/* Quick run history summary */}
                            <div className="pipeline-main-card">
                                <div className="pipeline-card-header">
                                    <div>
                                        <h3>Recent Runs</h3>
                                        <p className="pipeline-card-subtitle">
                                            Quick view of latest runs. Open the Runs tab for full details.
                                        </p>
                                    </div>
                                    <div className="pipeline-actions-row">
                                        <button
                                            className="pipeline-secondary-btn"
                                            type="button"
                                            onClick={() => setActiveSubtab("runs")}
                                        >
                                            View All Runs
                                        </button>
                                    </div>
                                </div>

                                <div className="pipeline-runs-list">
                                    {runs.length === 0 ? (
                                        <div className="pipeline-empty">No runs yet.</div>
                                    ) : (
                                        runs.slice(0, 5).map((run) => (
                                            <button
                                                key={run.id}
                                                type="button"
                                                className="pipeline-run-card"
                                                onClick={() => {
                                                    setActiveSubtab("runs");
                                                }}
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
                                                    {run.duration_seconds != null ? ` · ${run.duration_seconds}s` : ""}
                                                </div>
                                                <div className="pipeline-run-meta">
                                                    Started: {run.started_at || "-"}
                                                </div>
                                            </button>
                                        ))
                                    )}
                                </div>
                            </div>
                        </>
                    )}

                    {/* ========== RUNS SUBTAB ========== */}
                    {activeSubtab === "runs" && (
                        <div className="pipeline-main-card">
                            <PipelineRunsTab
                                pipelineId={selectedPipelineId}
                                pipelineName={selectedPipeline?.name}
                                connectionId={connectionId}
                                databaseName={databaseName}
                                selectedPipeline={selectedPipeline}
                                setSelectedPipeline={setSelectedPipeline}
                                onMessage={setMessage}
                                onError={setError}
                            />
                        </div>
                    )}

                    {/* ========== SCHEDULE SUBTAB ========== */}
                    {activeSubtab === "schedule" && (
                        <div className="pipeline-main-card">
                            <PipelineScheduleTab
                                pipelineId={selectedPipelineId}
                                pipelineName={selectedPipeline?.name}
                                onMessage={setMessage}
                                onError={setError}
                            />
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}