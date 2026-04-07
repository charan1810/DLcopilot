import { useCallback, useEffect, useState } from "react";
import {
    fetchPipelineSchedule,
    createPipelineSchedule,
    updatePipelineSchedule,
    deletePipelineSchedule,
} from "../api/schemaApi";

function formatTimestamp(ts) {
    if (!ts) return "-";
    try {
        return new Date(ts).toLocaleString();
    } catch {
        return ts;
    }
}

export default function PipelineScheduleTab({
    pipelineId,
    pipelineName,
    onMessage,
    onError,
}) {
    const [schedule, setSchedule] = useState(null);
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [deleting, setDeleting] = useState(false);
    const [hasSchedule, setHasSchedule] = useState(false);

    const DEFAULT_FORM = {
        schedule_type: "interval",
        cron_expression: "",
        interval_minutes: 60,
        is_active: 1,
    };

    const [form, setForm] = useState(DEFAULT_FORM);

    const loadSchedule = useCallback(async () => {
        if (!pipelineId) return;
        try {
            setLoading(true);
            const data = await fetchPipelineSchedule(pipelineId);
            if (data && data.id) {
                setSchedule(data);
                setHasSchedule(true);
                setForm({
                    schedule_type: data.schedule_type || "interval",
                    cron_expression: data.cron_expression || "",
                    interval_minutes: data.interval_minutes || 60,
                    is_active: data.is_active ?? 1,
                });
            } else if (data?.schedule && data.schedule.id) {
                setSchedule(data.schedule);
                setHasSchedule(true);
                setForm({
                    schedule_type: data.schedule.schedule_type || "interval",
                    cron_expression: data.schedule.cron_expression || "",
                    interval_minutes: data.schedule.interval_minutes || 60,
                    is_active: data.schedule.is_active ?? 1,
                });
            } else {
                setSchedule(null);
                setHasSchedule(false);
                setForm(DEFAULT_FORM);
            }
        } catch (err) {
            setSchedule(null);
            setHasSchedule(false);
            setForm(DEFAULT_FORM);
            // Surface real errors (not just "no schedule")
            const msg = err?.message || "";
            if (!msg.includes("404") && !msg.toLowerCase().includes("not found")) {
                onError?.(msg || "Failed to load schedule");
            }
        } finally {
            setLoading(false);
        }
    }, [pipelineId, onError]);

    useEffect(() => {
        loadSchedule();
    }, [loadSchedule]);

    async function handleSave() {
        if (!pipelineId) return;

        // Validate inputs
        if (form.schedule_type === "interval") {
            const mins = Number(form.interval_minutes);
            if (!Number.isFinite(mins) || mins < 1) {
                onError?.("Interval must be a number ≥ 1 minute.");
                return;
            }
        }
        if (form.schedule_type === "cron") {
            const expr = (form.cron_expression || "").trim();
            if (!expr) {
                onError?.("Cron expression is required.");
                return;
            }
        }

        try {
            setSaving(true);
            onError?.("");
            onMessage?.("");

            const payload = {
                schedule_type: form.schedule_type,
                cron_expression: form.schedule_type === "cron" ? form.cron_expression.trim() : null,
                interval_minutes: form.schedule_type === "interval" ? Number(form.interval_minutes) : null,
                is_active: form.is_active,
            };

            let result;
            if (hasSchedule) {
                result = await updatePipelineSchedule(pipelineId, payload);
            } else {
                result = await createPipelineSchedule(pipelineId, payload);
            }

            setSchedule(result);
            setHasSchedule(true);
            onMessage?.(hasSchedule ? "Schedule updated." : "Schedule created.");
            await loadSchedule();
        } catch (err) {
            onError?.(err.message || "Failed to save schedule");
        } finally {
            setSaving(false);
        }
    }

    async function handleDelete() {
        if (!pipelineId || !hasSchedule) return;
        const ok = window.confirm("Delete this schedule?");
        if (!ok) return;
        try {
            setDeleting(true);
            onError?.("");
            onMessage?.("");
            await deletePipelineSchedule(pipelineId);
            setSchedule(null);
            setHasSchedule(false);
            setForm(DEFAULT_FORM);
            onMessage?.("Schedule deleted.");
        } catch (err) {
            onError?.(err.message || "Failed to delete schedule");
        } finally {
            setDeleting(false);
        }
    }

    function handleToggleActive() {
        setForm((prev) => ({ ...prev, is_active: prev.is_active ? 0 : 1 }));
    }

    if (!pipelineId) {
        return (
            <div className="pipeline-empty">
                Select a pipeline to configure scheduling.
            </div>
        );
    }

    return (
        <div className="pipeline-schedule-tab">
            <div className="pipeline-card-header">
                <div>
                    <h3>Schedule{pipelineName ? `: ${pipelineName}` : ""}</h3>
                    <p className="pipeline-card-subtitle">
                        Configure automatic pipeline execution.
                    </p>
                </div>
                <div className="pipeline-actions-row">
                    <button
                        className="pipeline-secondary-btn"
                        type="button"
                        onClick={loadSchedule}
                        disabled={loading}
                    >
                        Refresh
                    </button>
                </div>
            </div>

            {loading ? (
                <div className="pipeline-empty">Loading schedule...</div>
            ) : (
                <>
                    {/* Current schedule status */}
                    {hasSchedule && schedule && (
                        <div className="pipeline-schedule-status-card">
                            <div className="pipeline-run-summary-grid" style={{ marginBottom: 0 }}>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Status</div>
                                    <div className="pipeline-summary-value">
                                        <span className={`run-status-badge ${schedule.is_active ? "run-status-success" : "run-status-failed"}`}>
                                            {schedule.is_active ? "ACTIVE" : "PAUSED"}
                                        </span>
                                    </div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Type</div>
                                    <div className="pipeline-summary-value" style={{ textTransform: "capitalize" }}>{schedule.schedule_type}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">
                                        {schedule.schedule_type === "cron" ? "Cron Expression" : "Interval"}
                                    </div>
                                    <div className="pipeline-summary-value">
                                        {schedule.schedule_type === "cron"
                                            ? schedule.cron_expression || "-"
                                            : `Every ${schedule.interval_minutes || "-"} min`}
                                    </div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Last Run</div>
                                    <div className="pipeline-summary-value pipeline-summary-value-sm">{formatTimestamp(schedule.last_run_at)}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Next Run</div>
                                    <div className="pipeline-summary-value pipeline-summary-value-sm">{formatTimestamp(schedule.next_run_at)}</div>
                                </div>
                            </div>
                        </div>
                    )}

                    {/* Schedule form */}
                    <div className="pipeline-schedule-form">
                        <div className="pipeline-form-grid">
                            <div className="form-field">
                                <label>Schedule Type</label>
                                <select
                                    value={form.schedule_type}
                                    onChange={(e) => setForm((prev) => ({ ...prev, schedule_type: e.target.value }))}
                                >
                                    <option value="interval">Interval (Every X Minutes)</option>
                                    <option value="cron">Cron Expression</option>
                                </select>
                            </div>

                            <div className="form-field">
                                <label>
                                    <input
                                        type="checkbox"
                                        checked={!!form.is_active}
                                        onChange={handleToggleActive}
                                        style={{ marginRight: "8px" }}
                                    />
                                    Schedule Active
                                </label>
                            </div>

                            {form.schedule_type === "interval" && (
                                <div className="form-field">
                                    <label>Interval (Minutes)</label>
                                    <input
                                        type="number"
                                        min="1"
                                        value={form.interval_minutes}
                                        onChange={(e) => setForm((prev) => ({ ...prev, interval_minutes: e.target.value }))}
                                        placeholder="e.g. 60"
                                    />
                                </div>
                            )}

                            {form.schedule_type === "cron" && (
                                <div className="form-field">
                                    <label>Cron Expression</label>
                                    <input
                                        type="text"
                                        value={form.cron_expression}
                                        onChange={(e) => setForm((prev) => ({ ...prev, cron_expression: e.target.value }))}
                                        placeholder="e.g. 0 */2 * * * (every 2 hours)"
                                    />
                                    <div className="pipeline-mini-muted" style={{ marginTop: "4px" }}>
                                        Format: minute hour day month day_of_week
                                    </div>
                                </div>
                            )}
                        </div>

                        <div className="pipeline-actions-row" style={{ marginTop: "16px" }}>
                            <button
                                className="pipeline-primary-btn"
                                type="button"
                                onClick={handleSave}
                                disabled={saving}
                            >
                                {saving ? "Saving..." : hasSchedule ? "Update Schedule" : "Save Schedule"}
                            </button>

                            {hasSchedule && (
                                <button
                                    className="pipeline-danger-btn"
                                    type="button"
                                    onClick={handleDelete}
                                    disabled={deleting}
                                >
                                    {deleting ? "Deleting..." : "Delete Schedule"}
                                </button>
                            )}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
