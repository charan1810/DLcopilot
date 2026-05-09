import { useEffect, useMemo, useRef, useState } from "react";
import {
    executeQuery,
    runAgenticInsights,
    saveConnection,
} from "../api/schemaApi";
import { useAppContext } from "../context/AppContext";
import { useAuth } from "../context/AuthContext";
import "./InsightsPage.css";

function formatCellValue(value) {
    if (value === null || value === undefined) return "";
    if (typeof value === "object") {
        try {
            return JSON.stringify(value);
        } catch {
            return String(value);
        }
    }
    return String(value);
}

function buildAnswerSummary(result) {
    const columns = result?.columns || [];
    const rows = result?.rows || [];

    if (!columns.length || !rows.length) {
        return null;
    }

    if (rows.length === 1) {
        const row = rows[0] || [];
        const rowPairs = columns.slice(0, row.length).map((column, index) => ({
            column,
            value: formatCellValue(row[index]),
        }));

        if (rowPairs.length === 1) {
            return {
                headline: `${rowPairs[0].column}: ${rowPairs[0].value}`,
                highlights: [],
            };
        }

        return {
            headline: rowPairs.slice(0, 4).map((item) => `${item.column}: ${item.value}`).join(" | "),
            highlights: [],
        };
    }

    const numericIndexes = [];
    columns.forEach((_, index) => {
        const hasNumeric = rows.some((row) => Number.isFinite(Number(row[index])));
        if (hasNumeric) numericIndexes.push(index);
    });

    const metricIndex = numericIndexes[0] ?? -1;
    const dimensionIndex = columns.findIndex((_, index) => !numericIndexes.includes(index));

    if (metricIndex >= 0 && dimensionIndex >= 0) {
        return {
            headline: `Found ${rows.length} ${columns[dimensionIndex]} values with ${columns[metricIndex]} results.`,
            highlights: rows.slice(0, 5).map((row) => `${formatCellValue(row[dimensionIndex])}: ${formatCellValue(row[metricIndex])}`),
        };
    }

    return {
        headline: `Returned ${rows.length} rows.`,
        highlights: columns.slice(0, 4).map((column, index) => `${column}: ${formatCellValue(rows[0]?.[index])}`),
    };
}

function InsightsResultTable({ result }) {
    if (!result) {
        return <div className="object-details-empty">No result preview yet.</div>;
    }

    if (!Array.isArray(result.rows) || !result.rows.length) {
        return <div className="object-details-empty">Query ran successfully, but no rows were returned.</div>;
    }

    return (
        <div className="sample-table-wrapper insights-results-table-wrap">
            <table className="sample-table">
                <thead>
                    <tr>
                        {(result.columns || []).map((col) => (
                            <th key={col}>{col}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {result.rows.map((row, rowIndex) => (
                        <tr key={rowIndex}>
                            {row.map((cell, cellIndex) => (
                                <td key={`${rowIndex}-${cellIndex}`} title={formatCellValue(cell)}>
                                    {formatCellValue(cell)}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

function detectInsights(result) {
    const columns = result?.columns || [];
    const rows = result?.rows || [];

    if (!rows.length || !columns.length) {
        return {
            stats: null,
            chart: [],
            chartLabel: "",
            recommendations: [],
        };
    }

    const numericIndexes = [];
    columns.forEach((_, index) => {
        const hasNumeric = rows.some((row) => Number.isFinite(Number(row[index])));
        if (hasNumeric) numericIndexes.push(index);
    });

    const dimensionIndex = columns.findIndex((_, index) => !numericIndexes.includes(index));
    const metricIndex = numericIndexes[0] ?? -1;

    let stats = null;
    let chart = [];
    let chartLabel = "";

    if (metricIndex >= 0) {
        const values = rows
            .map((row) => Number(row[metricIndex]))
            .filter((value) => Number.isFinite(value));

        if (values.length) {
            const sum = values.reduce((acc, value) => acc + value, 0);
            const avg = sum / values.length;
            const min = Math.min(...values);
            const max = Math.max(...values);

            stats = {
                metric: columns[metricIndex],
                count: values.length,
                sum,
                avg,
                min,
                max,
            };
        }

        const rawChart = rows.slice(0, 8).map((row, idx) => ({
            label:
                dimensionIndex >= 0
                    ? formatCellValue(row[dimensionIndex]) || `Row ${idx + 1}`
                    : `Row ${idx + 1}`,
            value: Number(row[metricIndex]),
        }));

        chart = rawChart.filter((item) => Number.isFinite(item.value));
        chartLabel = `${columns[metricIndex]} by ${dimensionIndex >= 0 ? columns[dimensionIndex] : "row"
            }`;
    }

    const recommendations = [];
    const loweredColumns = columns.map((col) => String(col).toLowerCase());
    const hasSales = loweredColumns.some((col) => col.includes("sales") || col.includes("revenue"));
    const hasInventory = loweredColumns.some((col) => col.includes("stock") || col.includes("inventory"));
    const hasAge = loweredColumns.some((col) => col.includes("age") || col.includes("old"));

    if (hasSales && hasInventory) {
        recommendations.push("Prioritize high-stock and low-sales items for discount bundles or targeted campaigns.");
    }
    if (hasAge) {
        recommendations.push("Segment aging products by margin and run a markdown ladder with weekly performance checks.");
    }
    if (!recommendations.length && stats) {
        recommendations.push("Use the top and bottom segments from this result as candidates for pricing and channel experiments.");
    }

    return {
        stats,
        chart,
        chartLabel,
        recommendations,
    };
}

function InsightsStats({ stats }) {
    if (!stats) return null;

    return (
        <div className="insights-stats-grid">
            <div className="insights-stat-card">
                <span>Metric</span>
                <strong>{stats.metric}</strong>
            </div>
            <div className="insights-stat-card">
                <span>Avg</span>
                <strong>{stats.avg.toFixed(2)}</strong>
            </div>
            <div className="insights-stat-card">
                <span>Min</span>
                <strong>{stats.min.toFixed(2)}</strong>
            </div>
            <div className="insights-stat-card">
                <span>Max</span>
                <strong>{stats.max.toFixed(2)}</strong>
            </div>
            <div className="insights-stat-card">
                <span>Sum</span>
                <strong>{stats.sum.toFixed(2)}</strong>
            </div>
            <div className="insights-stat-card">
                <span>Rows</span>
                <strong>{stats.count}</strong>
            </div>
        </div>
    );
}

function InsightsChart({ chart, label }) {
    if (!chart?.length) return null;

    const maxValue = Math.max(...chart.map((item) => item.value));

    return (
        <div className="insights-chart-wrap">
            <h4>{label}</h4>
            <div className="insights-chart">
                {chart.map((point) => {
                    const width = maxValue > 0 ? Math.max((point.value / maxValue) * 100, 4) : 4;
                    return (
                        <div className="insights-chart-row" key={`${point.label}-${point.value}`}>
                            <span className="insights-chart-label" title={point.label}>{point.label}</span>
                            <div className="insights-chart-bar-track">
                                <div className="insights-chart-bar" style={{ width: `${width}%` }} />
                            </div>
                            <span className="insights-chart-value">{point.value.toFixed(2)}</span>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function deriveConfidence(message) {
    const warnings = Array.isArray(message?.generationMeta?.warnings)
        ? message.generationMeta.warnings
        : [];
    const retrievedObjects = Array.isArray(message?.retrievalContext?.retrieved_objects)
        ? message.retrievalContext.retrieved_objects.length
        : 0;
    const relationshipHints = Array.isArray(message?.retrievalContext?.relationship_hints)
        ? message.retrievalContext.relationship_hints.length
        : 0;
    const rowCount = Array.isArray(message?.result?.rows) ? message.result.rows.length : 0;

    let score = 0.45;
    if (message?.generationMeta?.previewable) score += 0.2;
    if (rowCount > 0) score += 0.15;
    if (retrievedObjects >= 2) score += 0.1;
    if (relationshipHints > 0) score += 0.05;
    if (warnings.length === 0) {
        score += 0.05;
    } else {
        score -= Math.min(0.15, warnings.length * 0.05);
    }
    if (message?.generationMeta?.llm_enabled === false) score -= 0.05;

    const normalized = clamp(score, 0.05, 0.98);
    const tone = normalized >= 0.8 ? "high" : normalized >= 0.6 ? "medium" : "low";
    const label = tone === "high" ? "High confidence" : tone === "medium" ? "Moderate confidence" : "Low confidence";

    return {
        score: normalized,
        tone,
        label,
        warningCount: warnings.length,
        retrievedObjects,
        relationshipHints,
        rowCount,
    };
}

function LoadingAssistantBubble({ hint, stageText }) {
    return (
        <article className="insights-bubble insights-bubble-assistant insights-loading-bubble">
            <div className="insights-bubble-head">Insights Agent</div>
            <p className="insights-bubble-text">{hint}</p>
            <div className="insights-loading-stage">{stageText}</div>
            <div className="insights-loading-dots" aria-hidden="true">
                <span />
                <span />
                <span />
            </div>
        </article>
    );
}

function buildConversationTurn(message, fallbackContext = null) {
    const turn = {
        role: message.role,
        text: message.text,
    };

    const context = message.context || fallbackContext;
    if (!context) {
        return turn;
    }

    turn.context = context;

    if (context.database) turn.database_name = context.database;
    if (context.schema) turn.schema_name = context.schema;
    if (context.object) turn.object_name = context.object;
    if (Array.isArray(context.selected_objects) && context.selected_objects.length) {
        turn.selected_objects = context.selected_objects;
    }

    return turn;
}

function buildConversationHistory(messages, currentMessage, fallbackContext = null) {
    return [...messages, currentMessage]
        .slice(-10)
        .map((item, index, collection) => {
            const isLatestTurn = index === collection.length - 1;
            return buildConversationTurn(item, isLatestTurn ? fallbackContext : null);
        });
}

const INSIGHTS_SESSION_STORAGE_KEY = "dc.insightsChatSession";

function readInsightsSessionSnapshot() {
    try {
        const raw = window.sessionStorage.getItem(INSIGHTS_SESSION_STORAGE_KEY);
        if (!raw) return null;

        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === "object" ? parsed : null;
    } catch {
        return null;
    }
}

function writeInsightsSessionSnapshot(snapshot) {
    try {
        window.sessionStorage.setItem(INSIGHTS_SESSION_STORAGE_KEY, JSON.stringify(snapshot));
    } catch {
        // ignore storage failures
    }
}

function clearInsightsSessionSnapshot() {
    try {
        window.sessionStorage.removeItem(INSIGHTS_SESSION_STORAGE_KEY);
    } catch {
        // ignore storage failures
    }
}

function isValidInsightsMessage(message) {
    return !!message && typeof message === "object" && typeof message.role === "string" && typeof message.text === "string";
}

function buildInsightsScope({ user, connectionId, effectiveDatabase, defaultSchema }) {
    return {
        userKey: String(user?.id || user?.email || "anonymous"),
        connectionId: connectionId || null,
        database: effectiveDatabase || "",
        schema: defaultSchema || "",
    };
}

function isSameInsightsScope(left, right) {
    return (
        String(left?.userKey || "") === String(right?.userKey || "")
        && (left?.connectionId || null) === (right?.connectionId || null)
        && String(left?.database || "") === String(right?.database || "")
        && String(left?.schema || "") === String(right?.schema || "")
    );
}

function sanitizeInsightsSessionSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== "object") {
        return null;
    }

    return {
        scope: snapshot.scope && typeof snapshot.scope === "object" ? snapshot.scope : null,
        chatInput: typeof snapshot.chatInput === "string" ? snapshot.chatInput : "",
        messages: Array.isArray(snapshot.messages) ? snapshot.messages.filter(isValidInsightsMessage).slice(-20) : [],
        lastSql: typeof snapshot.lastSql === "string" ? snapshot.lastSql : "",
        lastResult: snapshot.lastResult && typeof snapshot.lastResult === "object" ? snapshot.lastResult : null,
        pendingDisambiguation:
            snapshot.pendingDisambiguation && typeof snapshot.pendingDisambiguation === "object"
                ? snapshot.pendingDisambiguation
                : null,
        chosenCandidates: Array.isArray(snapshot.chosenCandidates)
            ? snapshot.chosenCandidates.filter((item) => typeof item === "string")
            : [],
        conversationContext:
            snapshot.conversationContext && typeof snapshot.conversationContext === "object"
                ? snapshot.conversationContext
                : null,
    };
}

function AssistantMessage({ message }) {
    const confidence = deriveConfidence(message);
    const showEvidenceBadges =
        !!message?.generationMeta || !!message?.retrievalContext || !!message?.result;
    const hasAnalyticsPanel = !!message.detected?.stats || (message.detected?.chart || []).length > 0;
    const answerSummary = message?.answerSummary?.headline
        ? message.answerSummary
        : buildAnswerSummary(message.result);

    return (
        <article className="insights-bubble insights-bubble-assistant">
            <div className="insights-bubble-head">Insights Agent</div>

            <div className={`insights-response-layout ${hasAnalyticsPanel ? "" : "single-column"}`}>
                <section className="insights-summary-box">
                    {answerSummary?.headline ? (
                        <div className="insights-answer-box">
                            <div className="insights-answer-label">Quick answer</div>
                            <div className="insights-answer-headline">{answerSummary.headline}</div>
                            {Array.isArray(answerSummary.highlights) && answerSummary.highlights.length > 0 ? (
                                <div className="insights-answer-highlights">
                                    {answerSummary.highlights.map((item) => (
                                        <span key={item} className="insights-answer-chip">{item}</span>
                                    ))}
                                </div>
                            ) : null}
                        </div>
                    ) : null}

                    <p className="insights-bubble-text">{message.text}</p>

                    {showEvidenceBadges ? (
                        <div className="insights-trust-row">
                            <span className={`insights-confidence-badge tone-${confidence.tone}`}>
                                Confidence {Math.round(confidence.score * 100)}% · {confidence.label}
                            </span>
                            {confidence.retrievedObjects > 0 ? (
                                <span className="insights-evidence-chip">Objects: {confidence.retrievedObjects}</span>
                            ) : null}
                            {confidence.relationshipHints > 0 ? (
                                <span className="insights-evidence-chip">Relations: {confidence.relationshipHints}</span>
                            ) : null}
                            {confidence.rowCount > 0 ? (
                                <span className="insights-evidence-chip">Rows: {confidence.rowCount}</span>
                            ) : null}
                            {confidence.warningCount > 0 ? (
                                <span className="insights-evidence-chip warning">Warnings: {confidence.warningCount}</span>
                            ) : null}
                            {message?.generationMeta?.llm_enabled === false ? (
                                <span className="insights-evidence-chip warning">Fallback mode</span>
                            ) : null}
                        </div>
                    ) : null}

                    {message.generationMeta ? (
                        <div className="insights-meta-row">
                            <span className="context-pill">Type: {message.generationMeta.statement_type || "select"}</span>
                            <span className="context-pill">
                                {message.generationMeta.previewable ? "Previewable" : "Needs review"}
                            </span>
                        </div>
                    ) : null}

                    {Array.isArray(message?.generationMeta?.warnings) && message.generationMeta.warnings.length > 0 ? (
                        <div className="insights-list-wrap">
                            <h4>Execution Notes</h4>
                            <ul>
                                {message.generationMeta.warnings.map((item, index) => (
                                    <li key={`warn-${index}`}>{item}</li>
                                ))}
                            </ul>
                        </div>
                    ) : null}

                    {message.error ? <div className="object-details-error">{message.error}</div> : null}

                    {Array.isArray(message.detected?.recommendations) && message.detected.recommendations.length > 0 ? (
                        <div className="insights-list-wrap">
                            <h4>Recommendations</h4>
                            <ul>
                                {message.detected.recommendations.map((item, index) => (
                                    <li key={`rec-${index}`}>{item}</li>
                                ))}
                            </ul>
                        </div>
                    ) : null}

                    {message.sql ? (
                        <details className="insights-sql-details">
                            <summary>Generated SQL</summary>
                            <pre className="insights-sql-preview">{message.sql}</pre>
                        </details>
                    ) : null}
                </section>

                {hasAnalyticsPanel ? (
                    <section className="insights-analytics-box">
                        <h4 className="insights-panel-title">Statistics and Diagram</h4>
                        <InsightsStats stats={message.detected?.stats || null} />
                        <InsightsChart chart={message.detected?.chart || []} label={message.detected?.chartLabel || ""} />
                    </section>
                ) : null}
            </div>

            {message.result ? (
                <section className="insights-data-box">
                    <h4 className="insights-panel-title">Result Preview</h4>
                    <InsightsResultTable result={message.result} />
                </section>
            ) : null}
        </article>
    );
}

export default function InsightsPage() {
    const {
        connectionId,
        setConnectionId,
        connectionPayload,
        sessionPassword,
    } = useAppContext();
    const { user } = useAuth();

    const [chatInput, setChatInput] = useState("");
    const [messages, setMessages] = useState([]);
    const [isSending, setIsSending] = useState(false);
    const [globalError, setGlobalError] = useState("");
    const [lastSql, setLastSql] = useState("");
    const [lastResult, setLastResult] = useState(null);
    const [pendingDisambiguation, setPendingDisambiguation] = useState(null);
    const [chosenCandidates, setChosenCandidates] = useState([]);
    const [loadingHintIndex, setLoadingHintIndex] = useState(0);
    const [conversationContext, setConversationContext] = useState(null);

    const messagesRef = useRef(null);
    const restoreRef = useRef(false);

    const canUseAiFeatures = user?.role === "admin" || user?.role === "developer";
    const effectiveDatabase = connectionPayload?.database_name || "postgres";
    const defaultSchema = connectionPayload?.schema_name || "src";
    const insightsScope = useMemo(
        () => buildInsightsScope({ user, connectionId, effectiveDatabase, defaultSchema }),
        [user, connectionId, effectiveDatabase, defaultSchema]
    );

    const ensureActiveBackendConnection = async (forceNew = false) => {
        if (!forceNew && connectionId) return connectionId;

        if (!connectionPayload?.host || !connectionPayload?.username) {
            throw new Error("No active connection available. Save a connection in Explorer first.");
        }

        const payload = {
            ...connectionPayload,
            password: sessionPassword || "",
        };

        const saved = await saveConnection(payload);
        setConnectionId(saved.id);
        return saved.id;
    };

    const runWithReconnect = async (apiCall) => {
        let activeId = await ensureActiveBackendConnection(false);

        try {
            return await apiCall(activeId);
        } catch (err) {
            if ((err?.message || "").includes("Connection not found")) {
                activeId = await ensureActiveBackendConnection(true);
                return await apiCall(activeId);
            }
            throw err;
        }
    };

    const suggestedPrompts = useMemo(
        () => [
            "Which customer segments are driving revenue growth but margin decline in the last 2 quarters?",
            "Identify products with rising inventory days and falling sell-through, then rank markdown priority.",
            "What are the top drivers of failed payments by gateway, region, and hour of day?",
            "Compare CAC vs LTV by campaign and flag channels where spend is destroying value.",
            "Show cohort retention by signup month and highlight cohorts with unusual churn.",
            "Detect accounts with abnormal order frequency or average order value versus 12-week baseline.",
        ],
        []
    );

    const loadingHints = useMemo(
        () => [
            "Retrieving metadata and prior context...",
            "Planning SQL and business strategy...",
            "Running safe preview and summarizing evidence...",
            "Packaging charts, stats, and recommendations...",
        ],
        []
    );

    useEffect(() => {
        if (!isSending) {
            setLoadingHintIndex(0);
            return;
        }

        const intervalId = window.setInterval(() => {
            setLoadingHintIndex((prev) => (prev + 1) % loadingHints.length);
        }, 1200);

        return () => window.clearInterval(intervalId);
    }, [isSending, loadingHints.length]);

    useEffect(() => {
        if (restoreRef.current) {
            return;
        }

        const savedSnapshot = sanitizeInsightsSessionSnapshot(readInsightsSessionSnapshot());
        restoreRef.current = true;

        if (!savedSnapshot || !savedSnapshot.scope || !isSameInsightsScope(savedSnapshot.scope, insightsScope)) {
            clearInsightsSessionSnapshot();
            return;
        }

        setChatInput(savedSnapshot.chatInput || "");
        setMessages(savedSnapshot.messages || []);
        setLastSql(savedSnapshot.lastSql || "");
        setLastResult(savedSnapshot.lastResult || null);
        setPendingDisambiguation(savedSnapshot.pendingDisambiguation || null);
        setChosenCandidates(savedSnapshot.chosenCandidates || []);
        setConversationContext(savedSnapshot.conversationContext || null);
    }, [insightsScope]);

    useEffect(() => {
        if (!restoreRef.current) {
            return;
        }

        const currentSnapshot = sanitizeInsightsSessionSnapshot(readInsightsSessionSnapshot());
        if (currentSnapshot?.scope && !isSameInsightsScope(currentSnapshot.scope, insightsScope)) {
            setChatInput("");
            setMessages([]);
            setGlobalError("");
            setLastSql("");
            setLastResult(null);
            setPendingDisambiguation(null);
            setChosenCandidates([]);
            setConversationContext(null);
            clearInsightsSessionSnapshot();
        }
    }, [insightsScope]);

    useEffect(() => {
        if (!restoreRef.current) {
            return;
        }

        if (
            !chatInput
            && !messages.length
            && !lastSql
            && !pendingDisambiguation
            && !chosenCandidates.length
            && !conversationContext
            && !lastResult
        ) {
            clearInsightsSessionSnapshot();
            return;
        }

        writeInsightsSessionSnapshot({
            scope: insightsScope,
            chatInput,
            messages: messages.slice(-20),
            lastSql,
            lastResult,
            pendingDisambiguation,
            chosenCandidates,
            conversationContext,
        });
    }, [
        chatInput,
        chosenCandidates,
        conversationContext,
        insightsScope,
        lastResult,
        lastSql,
        messages,
        pendingDisambiguation,
    ]);

    const scrollToBottom = () => {
        window.setTimeout(() => {
            if (messagesRef.current) {
                messagesRef.current.scrollTop = messagesRef.current.scrollHeight;
            }
        }, 10);
    };

    const buildNarrative = (prompt, payload, result, detected, context = null) => {
        const lines = [];
        const scopedSchema = context?.schema || defaultSchema;
        const scopedObject = context?.object || "relevant objects";
        const scopedContext = `${scopedSchema}.${scopedObject}`;

        lines.push(`I analyzed your request: \"${prompt}\" on ${scopedContext}.`);

        if (payload?.explanation) {
            lines.push(payload.explanation);
        }

        if (detected?.stats) {
            lines.push(
                `The key metric ${detected.stats.metric} has average ${detected.stats.avg.toFixed(2)} with range ${detected.stats.min.toFixed(2)} to ${detected.stats.max.toFixed(2)}.`
            );
        }

        if (result?.rows?.length) {
            lines.push(`Returned ${result.rows.length} rows for immediate review.`);
        }

        if (Array.isArray(detected?.recommendations) && detected.recommendations.length) {
            lines.push("I included tactical recommendations below based on the output.");
        }

        return lines.join(" ");
    };

    const runInsightPipeline = async ({
        promptText,
        selectedObjects = [],
        schemaName,
        objectName,
        conversationHistory = [],
    }) => {
        const response = await runWithReconnect((activeId) =>
            runAgenticInsights({
                connection_id: activeId,
                database_name: effectiveDatabase,
                schema: schemaName || undefined,
                object_name: objectName || undefined,
                user_prompt: promptText,
                conversation_history: conversationHistory,
                selected_objects: selectedObjects,
            })
        );

        if (response?.mode === "needs_disambiguation") {
            const candidates = response?.disambiguation?.candidates || [];
            setPendingDisambiguation({
                prompt: promptText,
                candidates,
                conversationHistory,
            });
            setChosenCandidates([]);
            setMessages((prev) => [
                ...prev,
                {
                    id: Date.now() + 1,
                    role: "assistant",
                    text:
                        response?.message ||
                        "I found multiple matching objects. Please choose the relevant ones below.",
                },
            ]);
            return;
        }

        if (response?.mode === "out_of_scope") {
            setMessages((prev) => [
                ...prev,
                {
                    id: Date.now() + 1,
                    role: "assistant",
                    text:
                        response?.assistant_response ||
                        "I can help best with data analysis questions. Please ask about trends, comparisons, KPIs, or anomalies.",
                },
            ]);
            return;
        }

        const resolvedContext = response?.context || null;
        if (resolvedContext) {
            setConversationContext(resolvedContext);
        }

        const sql = response?.sql || "";
        setLastSql(sql);

        const result = response?.query_result || null;
        if (result) {
            setLastResult(result);
        }

        const backendInsights = response?.insights || {};
        const detected = backendInsights?.stats || backendInsights?.chart
            ? {
                stats: backendInsights?.stats || null,
                chart: backendInsights?.chart || [],
                chartLabel: backendInsights?.chart_label || "",
                recommendations: backendInsights?.recommendations || [],
            }
            : detectInsights(result || null);

        const generationMeta = response?.generation_meta || null;
        const retrievalContext = response?.retrieval_context || null;
        const answerSummary = response?.answer_summary?.headline
            ? response.answer_summary
            : buildAnswerSummary(result);
        const assistantMessage = {
            id: Date.now() + 1,
            role: "assistant",
            text:
                response?.assistant_response ||
                buildNarrative(promptText, generationMeta, result, detected, resolvedContext),
            answerSummary,
            context: resolvedContext,
            generationMeta,
            retrievalContext,
            sql,
            result,
            detected,
        };

        setMessages((prev) => [...prev, assistantMessage]);
    };

    const sendPrompt = async (promptText) => {
        const cleanPrompt = promptText.trim();
        if (!cleanPrompt || isSending) return;

        setGlobalError("");

        if (!canUseAiFeatures) {
            setGlobalError("Insights generation is available for admin and developer roles.");
            return;
        }

        const userMessage = {
            id: Date.now(),
            role: "user",
            text: cleanPrompt,
        };
        const historyWithCurrent = buildConversationHistory(messages, userMessage, conversationContext);

        setMessages((prev) => [...prev, userMessage]);
        setChatInput("");
        setPendingDisambiguation(null);
        setChosenCandidates([]);
        setIsSending(true);

        try {
            await runInsightPipeline({
                promptText: cleanPrompt,
                conversationHistory: historyWithCurrent,
            });
        } catch (err) {
            const assistantError = {
                id: Date.now() + 1,
                role: "assistant",
                text: "I could not complete this insight request.",
                error: err?.message || "Failed to generate insights",
            };
            setMessages((prev) => [...prev, assistantError]);
        } finally {
            setIsSending(false);
            scrollToBottom();
        }
    };

    const handleSubmit = (event) => {
        event.preventDefault();
        sendPrompt(chatInput);
    };

    const toggleCandidate = (candidateKey) => {
        setChosenCandidates((prev) =>
            prev.includes(candidateKey)
                ? prev.filter((key) => key !== candidateKey)
                : [...prev, candidateKey]
        );
    };

    const continueWithDisambiguation = async () => {
        if (!pendingDisambiguation || !chosenCandidates.length || isSending) return;

        setIsSending(true);
        setGlobalError("");

        try {
            const selectedMatches = pendingDisambiguation.candidates.filter((candidate) =>
                chosenCandidates.includes(`${candidate.schema}.${candidate.name}`)
            );

            if (!selectedMatches.length) {
                setGlobalError("Choose at least one object to continue.");
                setIsSending(false);
                return;
            }

            const primary = selectedMatches[0];
            const resolvedSchema = primary.schema || defaultSchema;
            const resolvedObject = primary.name || "";

            const selectedObjects = selectedMatches
                .map((candidate) => `${candidate.schema}.${candidate.name}`)
                .filter((reference) => reference !== `${resolvedSchema}.${resolvedObject}`);

            const disambiguatedPrompt = pendingDisambiguation.prompt;
            const disambiguatedHistory = pendingDisambiguation.conversationHistory || [];

            setPendingDisambiguation(null);
            setChosenCandidates([]);

            await runInsightPipeline({
                promptText: disambiguatedPrompt,
                selectedObjects,
                schemaName: resolvedSchema,
                objectName: resolvedObject,
                conversationHistory: disambiguatedHistory,
            });
        } catch (err) {
            setMessages((prev) => [
                ...prev,
                {
                    id: Date.now() + 3,
                    role: "assistant",
                    text: "I could not continue after disambiguation.",
                    error: err?.message || "Disambiguation run failed",
                },
            ]);
        } finally {
            setIsSending(false);
            scrollToBottom();
        }
    };

    const rerunLast = async () => {
        if (!lastSql?.trim()) {
            setGlobalError("No SQL available to rerun yet.");
            return;
        }

        try {
            const result = await runWithReconnect((activeId) =>
                executeQuery(activeId, effectiveDatabase, {
                    query: lastSql,
                    limit: 100,
                    offset: 0,
                })
            );

            setLastResult(result || null);
            const detected = detectInsights(result || null);
            setMessages((prev) => [
                ...prev,
                {
                    id: Date.now() + 2,
                    role: "assistant",
                    text: "I re-ran the latest SQL and refreshed the evidence below.",
                    answerSummary: buildAnswerSummary(result),
                    context: conversationContext,
                    sql: lastSql,
                    result,
                    detected,
                },
            ]);
            scrollToBottom();
        } catch (err) {
            setGlobalError(err?.message || "Failed to rerun SQL");
        }
    };

    const startNewChat = () => {
        setChatInput("");
        setMessages([]);
        setGlobalError("");
        setLastSql("");
        setLastResult(null);
        setPendingDisambiguation(null);
        setChosenCandidates([]);
        setConversationContext(null);
        clearInsightsSessionSnapshot();
    };

    return (
        <div className="page insights-page insights-chat-page">
            <div className="module-hero compact-hero insights-hero insights-chat-hero">
                <div>
                    <div className="module-badge">Insights Agent</div>
                    <h1 className="module-title">Ask Business Questions</h1>
                    <p className="module-subtitle">
                        Chat with your data context. The assistant will generate SQL, compute statistics,
                        and render quick charts when possible.
                    </p>
                </div>
                <div className="insights-hero-actions">
                    <button
                        type="button"
                        className="secondary-btn"
                        onClick={startNewChat}
                        disabled={isSending || (!messages.length && !chatInput && !lastSql && !pendingDisambiguation)}
                    >
                        New Chat
                    </button>
                    <button type="button" className="secondary-btn" onClick={rerunLast} disabled={isSending}>
                        Re-run Last Insight
                    </button>
                </div>
            </div>

            {globalError ? <div className="object-details-error">{globalError}</div> : null}

            <section className="insights-chat-shell">
                <div className="insights-chat-suggestions">
                    {suggestedPrompts.map((item) => (
                        <button
                            key={item}
                            type="button"
                            className="insights-suggestion-chip"
                            onClick={() => sendPrompt(item)}
                            disabled={isSending}
                        >
                            {item}
                        </button>
                    ))}
                </div>

                <div className="insights-chat-messages" ref={messagesRef}>
                    {messages.length === 0 ? (
                        <div className="insights-chat-empty">
                            Ask anything about your data. I will infer relevant objects, generate SQL, and return insights with quick statistics and charting.
                        </div>
                    ) : null}

                    {messages.map((message) =>
                        message.role === "user" ? (
                            <article key={message.id} className="insights-bubble insights-bubble-user">
                                <div className="insights-bubble-head">You</div>
                                <p className="insights-bubble-text">{message.text}</p>
                            </article>
                        ) : (
                            <AssistantMessage key={message.id} message={message} />
                        )
                    )}

                    {isSending ? (
                        <LoadingAssistantBubble
                            hint={loadingHints[loadingHintIndex]}
                            stageText={`Stage ${loadingHintIndex + 1} of ${loadingHints.length}`}
                        />
                    ) : null}
                </div>

                {pendingDisambiguation ? (
                    <div className="insights-disambiguation-panel">
                        <div className="insights-disambiguation-head">
                            <h4>Disambiguation Needed</h4>
                            <p className="section-caption">
                                I found multiple matching objects for this request. Select the relevant ones.
                            </p>
                        </div>
                        <div className="insights-disambiguation-options">
                            {pendingDisambiguation.candidates.map((candidate) => {
                                const candidateKey = `${candidate.schema}.${candidate.name}`;
                                const active = chosenCandidates.includes(candidateKey);
                                return (
                                    <button
                                        type="button"
                                        key={candidateKey}
                                        className={`insights-disambiguation-chip ${active ? "active" : ""}`}
                                        onClick={() => toggleCandidate(candidateKey)}
                                    >
                                        {candidate.schema}.{candidate.name} ({candidate.type})
                                    </button>
                                );
                            })}
                        </div>
                        <div className="insights-disambiguation-actions">
                            <button
                                type="button"
                                className="primary-btn"
                                onClick={continueWithDisambiguation}
                                disabled={!chosenCandidates.length || isSending}
                            >
                                Continue with Selection
                            </button>
                            <button
                                type="button"
                                className="secondary-btn"
                                onClick={() => {
                                    setPendingDisambiguation(null);
                                    setChosenCandidates([]);
                                }}
                                disabled={isSending}
                            >
                                Cancel
                            </button>
                        </div>
                    </div>
                ) : null}

                <form className="insights-composer" onSubmit={handleSubmit}>
                    <input
                        type="text"
                        className="insights-composer-input"
                        value={chatInput}
                        onChange={(e) => setChatInput(e.target.value)}
                        placeholder="Ask for strategies, anomalies, demand patterns, cohort trends..."
                    />
                    <button type="submit" className="primary-btn" disabled={isSending || !chatInput.trim()}>
                        {isSending ? "Thinking..." : "Send"}
                    </button>
                </form>
            </section>

            {/* {lastResult ? (
                <section className="insights-card">
                    <div className="compact-section-header">
                        <div>
                            <h3>Latest Result Snapshot</h3>
                            <p className="section-caption">Quick access to the most recent query output.</p>
                        </div>
                    </div>
                    <InsightsResultTable result={lastResult} />
                </section>
            ) : null} */}
        </div>
    );
}
