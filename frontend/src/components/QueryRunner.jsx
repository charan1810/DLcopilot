import { useEffect, useMemo, useState } from "react";
import { executeQuery, fixSqlQuery } from "../api/schemaApi";
import { useAppContext } from "../context/AppContext";
import "./QueryRunner.css";

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

export default function QueryRunner() {
    const {
        connectionId,
        selectedDatabase,
        selectedSchema,
        selectedObject,
        queryRunnerQuery,
        setQueryRunnerQuery,
    } = useAppContext();

    const [query, setQuery] = useState("");
    const [limit, setLimit] = useState(25);
    const [offset, setOffset] = useState(0);
    const [result, setResult] = useState(null);
    const [running, setRunning] = useState(false);
    const [error, setError] = useState("");
    const [copied, setCopied] = useState(false);
    const [fixLoading, setFixLoading] = useState(false);
    const [fixResult, setFixResult] = useState(null);

    useEffect(() => {
        if (typeof queryRunnerQuery === "string" && queryRunnerQuery.trim()) {
            setQuery(queryRunnerQuery);
            setOffset(0);
            setError("");
        }
    }, [queryRunnerQuery]);

    const totalPages = useMemo(() => {
        if (!result?.total_count) return 1;
        return Math.max(1, Math.ceil(result.total_count / limit));
    }, [result, limit]);

    const currentPage = useMemo(() => Math.floor(offset / limit) + 1, [offset, limit]);

    const canRun = !!connectionId && !!selectedDatabase && !!query.trim();

    const runQuery = async (nextOffset = offset, nextLimit = limit) => {
        if (!canRun) return;

        setRunning(true);
        setError("");
        setFixResult(null);

        try {
            const data = await executeQuery(connectionId, selectedDatabase, {
                query,
                limit: nextLimit,
                offset: nextOffset,
            });
            setResult(data);
        } catch (err) {
            setError(err.message || "Failed to execute query");
            setResult(null);
        } finally {
            setRunning(false);
        }
    };

    const handleRun = async () => {
        setOffset(0);
        setQueryRunnerQuery(query);
        await runQuery(0, limit);
    };

    const handleLimitChange = async (e) => {
        const newLimit = Number(e.target.value);
        setLimit(newLimit);
        setOffset(0);

        if (query.trim() && result) {
            await runQuery(0, newLimit);
        }
    };

    const handlePrev = async () => {
        const newOffset = Math.max(0, offset - limit);
        setOffset(newOffset);
        await runQuery(newOffset, limit);
    };

    const handleNext = async () => {
        const newOffset = offset + limit;
        setOffset(newOffset);
        await runQuery(newOffset, limit);
    };

    const handleClear = () => {
        setQuery("");
        setOffset(0);
        setResult(null);
        setError("");
        setFixResult(null);
        setQueryRunnerQuery("");
    };

    const handleCopyQuery = async () => {
        if (!query.trim()) return;

        try {
            await navigator.clipboard.writeText(query);
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1500);
        } catch (err) {
            console.error("Failed to copy query:", err);
        }
    };

    const handleFixWithAI = async () => {
        if (!query || !error) return;

        setFixLoading(true);
        setFixResult(null);

        try {
            const data = await fixSqlQuery(
                connectionId,
                selectedDatabase,
                query,
                error,
                {
                    schema: selectedSchema || "public",
                    object_name: selectedObject || "",
                }
            );
            setFixResult(data);
        } catch (err) {
            console.error("AI Fix failed:", err);
        } finally {
            setFixLoading(false);
        }
    };

    return (
        <div className="query-runner">
            <div className="query-runner-shell">
                <div className="query-runner-header-card">
                    <div className="query-runner-header-top">
                        <div>
                            <div className="module-badge">Engineering Flow</div>
                            <h2 className="query-runner-title">Query Runner</h2>
                            <p className="query-runner-subtitle">
                                Run SQL and debug errors using AI-assisted fixes.
                            </p>
                        </div>

                        <div className="query-runner-context">
                            <span className="context-pill">
                                DB: {selectedDatabase || "Not selected"}
                            </span>
                        </div>
                    </div>
                </div>

                <div className="query-runner-editor-card">
                    <div className="query-section-header">
                        <div>
                            <h3>SQL Editor</h3>
                            <p className="section-caption" style={{ marginTop: "6px" }}>
                                Paste SQL manually or send generated SQL here from Transformations or recipes.
                            </p>
                        </div>
                    </div>

                    <textarea
                        className="query-textarea"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                    />

                    <div className="query-toolbar">
                        <div className="query-toolbar-left">
                            <div className="form-field query-limit-field">
                                <label>Rows per page</label>
                                <select
                                    value={limit}
                                    onChange={handleLimitChange}
                                    className="limit-select"
                                >
                                    <option value={25}>25 rows</option>
                                    <option value={50}>50 rows</option>
                                    <option value={100}>100 rows</option>
                                </select>
                            </div>
                        </div>

                        <div className="query-toolbar-right">
                            <button className="primary-btn" onClick={handleRun} disabled={running || !canRun}>
                                {running ? "Running..." : "Run Query"}
                            </button>

                            <button className="secondary-btn" onClick={handleCopyQuery} disabled={!query.trim()}>
                                {copied ? "Copied" : "Copy SQL"}
                            </button>

                            <button className="secondary-btn" onClick={handleClear}>
                                Clear
                            </button>
                        </div>
                    </div>

                    {error && (
                        <div className="object-details-error" style={{ marginTop: "14px" }}>
                            <div>{error}</div>

                            <button
                                className="primary-btn"
                                onClick={handleFixWithAI}
                                disabled={fixLoading}
                                style={{ marginTop: "10px" }}
                            >
                                {fixLoading ? "Fixing..." : "Fix with AI"}
                            </button>
                        </div>
                    )}
                </div>

                {fixResult && (
                    <div className="query-runner-results-card">
                        <div className="query-section-header">
                            <div>
                                <h3>AI Suggested Fix</h3>
                                {fixResult.explanation ? (
                                    <p className="section-caption" style={{ marginTop: "6px" }}>
                                        {fixResult.explanation}
                                    </p>
                                ) : null}
                            </div>
                        </div>

                        <textarea
                            className="query-textarea"
                            value={fixResult.sql || ""}
                            readOnly
                        />

                        <div className="query-toolbar" style={{ marginTop: "12px" }}>
                            <div className="query-toolbar-right">
                                <button
                                    className="primary-btn"
                                    onClick={() => setQuery(fixResult.sql || "")}
                                >
                                    Use Fixed SQL
                                </button>
                            </div>
                        </div>

                        {Array.isArray(fixResult.warnings) && fixResult.warnings.length > 0 ? (
                            <div className="object-details-error" style={{ marginTop: "12px" }}>
                                {fixResult.warnings.map((w, i) => (
                                    <div key={i}>Warning: {w}</div>
                                ))}
                            </div>
                        ) : null}

                        {Array.isArray(fixResult.agent_log) && fixResult.agent_log.length > 0 ? (
                            <details className="pipeline-sql-details" style={{ marginTop: "12px" }}>
                                <summary>Agent Validation Log ({fixResult.agent_log.length} steps)</summary>
                                <div style={{ padding: "10px 12px" }}>
                                    {fixResult.agent_log.map((entry, i) => (
                                        <div key={i} style={{ fontSize: "12px", marginBottom: "6px" }}>
                                            [{entry.phase}] {entry.details}
                                        </div>
                                    ))}
                                </div>
                            </details>
                        ) : null}
                    </div>
                )}

                <div className="query-runner-results-card">
                    <div className="query-section-header query-results-header">
                        <div>
                            <h3>Results</h3>
                            <p className="section-caption" style={{ marginTop: "6px" }}>
                                Preview returned rows with pagination support.
                            </p>
                        </div>

                        {result ? (
                            <div className="results-meta-pills">
                                <span className="context-pill">Total Rows: {result.total_count ?? 0}</span>
                                <span className="context-pill">Page {currentPage} of {totalPages}</span>
                                <span className="context-pill">Showing: {result.rows?.length ?? 0}</span>
                            </div>
                        ) : null}
                    </div>

                    {result ? (
                        <>
                            {Array.isArray(result.rows) && result.rows.length > 0 ? (
                                <div className="sample-table-wrapper query-results-table-wrap">
                                    <table className="sample-table query-results-table">
                                        <thead>
                                            <tr>
                                                {result.columns.map((col) => (
                                                    <th key={col}>{col}</th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {result.rows.map((row, i) => (
                                                <tr key={i}>
                                                    {row.map((cell, j) => (
                                                        <td key={j} title={formatCellValue(cell)}>
                                                            {formatCellValue(cell)}
                                                        </td>
                                                    ))}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            ) : (
                                <div className="object-details-empty">
                                    Query executed successfully, but no rows were returned.
                                </div>
                            )}

                            <div className="pagination-row pagination-row-enhanced">
                                <button
                                    type="button"
                                    className="secondary-btn"
                                    onClick={handlePrev}
                                    disabled={offset === 0 || running}
                                >
                                    Previous
                                </button>

                                <div className="pagination-status">
                                    Offset: {offset} | Limit: {limit}
                                </div>

                                <button
                                    type="button"
                                    className="secondary-btn"
                                    onClick={handleNext}
                                    disabled={running || offset + limit >= (result.total_count || 0)}
                                >
                                    Next
                                </button>
                            </div>
                        </>
                    ) : (
                        <div className="object-details-empty">
                            No query results yet.
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}