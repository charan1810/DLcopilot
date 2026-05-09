import { useMemo, useState } from "react";
import {
    benchmarkView,
    dedupAdvanced,
    inferPrimaryKeys,
    optimizeQuery,
    queryColumns,
} from "../api/envToolsApi";
import { useAppContext } from "../context/AppContext";
import "./EnvComparator.css";
import "./Optimizer.css";

async function copyToClipboard(text) {
    const value = String(text || "");
    if (!value) return false;
    try {
        if (navigator?.clipboard?.writeText) {
            await navigator.clipboard.writeText(value);
            return true;
        }
    } catch {
        // Ignore clipboard errors.
    }
    return false;
}

function parseCommaList(value) {
    return String(value || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
}

export default function Optimizer() {
    const {
        connectionId,
        connectionPayload,
        selectedDatabase,
        selectedSchema,
        selectedObject,
    } = useAppContext();

    const databaseName = selectedDatabase || connectionPayload?.database_name || "dlcopilot";
    const schemaName = selectedSchema || connectionPayload?.schema_name || "src";

    const [error, setError] = useState("");
    const [info, setInfo] = useState("");

    const [sql, setSql] = useState("");
    const [columns, setColumns] = useState([]);
    const [pkCandidates, setPkCandidates] = useState([]);
    const [selectedPkCols, setSelectedPkCols] = useState([]);

    const [optimizedSql, setOptimizedSql] = useState("");
    const [optExplain, setOptExplain] = useState(null);

    const [dedupStrategy, setDedupStrategy] = useState("LATEST_PER_PK");
    const [hashExclude, setHashExclude] = useState("");
    const [exposeHash, setExposeHash] = useState(false);
    const [hashColName, setHashColName] = useState("ROW_HASH");

    const [targetSchema, setTargetSchema] = useState("core");
    const [targetTable, setTargetTable] = useState(
        selectedObject ? `${selectedObject}_s` : ""
    );

    const [dedupSql, setDedupSql] = useState("");
    const [insertSql, setInsertSql] = useState("");
    const [dedupChecks, setDedupChecks] = useState(null);
    const [benchmark, setBenchmark] = useState(null);

    const [loadingColumns, setLoadingColumns] = useState(false);
    const [loadingPk, setLoadingPk] = useState(false);
    const [loadingOptimize, setLoadingOptimize] = useState(false);
    const [loadingDedup, setLoadingDedup] = useState(false);
    const [loadingBenchmark, setLoadingBenchmark] = useState(false);

    const hasConnectionContext = !!connectionId && !!databaseName;

    const requestContext = useMemo(
        () => ({
            connection_id: connectionId,
            database_name: databaseName,
            schema: schemaName,
        }),
        [connectionId, databaseName, schemaName]
    );

    const selectedPkLabel = useMemo(() => selectedPkCols.join(", "), [selectedPkCols]);

    const ensureReady = () => {
        if (!hasConnectionContext) {
            throw new Error("Save and select a PostgreSQL connection first from Explorer.");
        }
        if (!sql.trim()) {
            throw new Error("Enter a SELECT or WITH SQL query first.");
        }
    };

    const handleQueryColumns = async () => {
        try {
            setError("");
            setInfo("");
            setLoadingColumns(true);
            ensureReady();

            const response = await queryColumns({
                ...requestContext,
                sql: sql.trim(),
            });

            if (!response.data?.success) {
                throw new Error(response.data?.error || "Failed to infer columns.");
            }

            const resultColumns = response.data.columns || [];
            setColumns(resultColumns);
            setInfo(`Detected ${resultColumns.length} output column(s).`);
        } catch (err) {
            setError(err?.message || "Failed to infer columns.");
        } finally {
            setLoadingColumns(false);
        }
    };

    const handleInferPk = async () => {
        try {
            setError("");
            setInfo("");
            setLoadingPk(true);
            ensureReady();

            const response = await inferPrimaryKeys({
                ...requestContext,
                sql: sql.trim(),
            });

            if (!response.data?.success) {
                throw new Error(response.data?.error || "Failed to infer primary keys.");
            }

            const resultColumns = response.data.columns || [];
            const candidates = response.data.candidates || [];
            setColumns(resultColumns);
            setPkCandidates(candidates);

            if (candidates.length && !selectedPkCols.length) {
                setSelectedPkCols(candidates[0].columns || []);
            }

            setInfo(`Found ${candidates.length} PK candidate(s).`);
        } catch (err) {
            setError(err?.message || "Failed to infer primary keys.");
        } finally {
            setLoadingPk(false);
        }
    };

    const handleOptimize = async () => {
        try {
            setError("");
            setInfo("");
            setLoadingOptimize(true);
            ensureReady();

            const response = await optimizeQuery({
                ...requestContext,
                sql: sql.trim(),
            });

            if (!response.data?.success) {
                throw new Error(response.data?.error || "Optimization failed.");
            }

            setOptimizedSql(response.data.optimized || "");
            setOptExplain(response.data.explain || null);
            setBenchmark(null);
            const changeSummary = (response.data.explain?.changes || []).join(" ").trim();
            setInfo(changeSummary || "Optimization completed.");
        } catch (err) {
            setError(err?.message || "Optimization failed.");
        } finally {
            setLoadingOptimize(false);
        }
    };

    const handleDedup = async () => {
        try {
            setError("");
            setInfo("");
            setLoadingDedup(true);
            ensureReady();

            if (!selectedPkCols.length) {
                throw new Error("Select at least one PK column before generating dedup SQL.");
            }

            const response = await dedupAdvanced({
                ...requestContext,
                sql: sql.trim(),
                pkCols: selectedPkCols,
                strategy: dedupStrategy,
                hashExcludeCols: parseCommaList(hashExclude),
                exposeHash,
                hashColName,
                targetSchema: targetSchema.trim() || undefined,
                targetTable: targetTable.trim() || undefined,
            });

            if (!response.data?.success) {
                throw new Error(response.data?.error || "Failed to generate dedup SQL.");
            }

            setDedupSql(response.data.deduped || "");
            setInsertSql(response.data.insertSql || "");
            setDedupChecks(response.data.checks || null);

            if (response.data?.checks?.isDedupValid) {
                setInfo("Dedup SQL generated and validated with duplicate-group checks.");
            } else {
                setInfo("Dedup SQL generated, but duplicate checks indicate remaining duplicate groups.");
            }
        } catch (err) {
            setError(err?.message || "Failed to generate dedup SQL.");
        } finally {
            setLoadingDedup(false);
        }
    };

    const handleBenchmark = async () => {
        try {
            setError("");
            setInfo("");
            setLoadingBenchmark(true);

            ensureReady();
            if (!optimizedSql.trim()) {
                throw new Error("Generate optimized SQL first.");
            }

            const response = await benchmarkView({
                ...requestContext,
                originalSql: sql.trim(),
                optimizedSql: optimizedSql.trim(),
            });

            if (!response.data?.success) {
                throw new Error(response.data?.error || "Benchmark failed.");
            }

            setBenchmark(response.data);
            setInfo("Benchmark completed.");
        } catch (err) {
            setError(err?.message || "Benchmark failed.");
        } finally {
            setLoadingBenchmark(false);
        }
    };

    const togglePk = (columnName) => {
        setSelectedPkCols((prev) => (
            prev.includes(columnName)
                ? prev.filter((item) => item !== columnName)
                : [...prev, columnName]
        ));
    };

    return (
        <div className="page">
            <div className="module-hero compact-hero">
                <div className="module-badge">Utilities</div>
                <h1 className="module-title">SQL Optimizer</h1>
                <p className="module-subtitle">
                    PostgreSQL optimizer and dedup SQL builder with idempotent insert generation.
                </p>
            </div>

            {!hasConnectionContext ? (
                <div className="pipeline-main-card envtools-card">
                    <div className="pipeline-empty">
                        Save and select a PostgreSQL connection in Explorer first, then reopen this tab.
                    </div>
                </div>
            ) : (
                <>
                    {error ? <div className="pipeline-inline-banner error">{error}</div> : null}
                    {info ? <div className="pipeline-inline-banner success">{info}</div> : null}

                    <div className="pipeline-main-card envtools-card">
                        <div className="pipeline-card-header">
                            <div>
                                <h3>Query Input</h3>
                                <p className="pipeline-card-subtitle">
                                    Database: {databaseName} | Schema: {schemaName} | Connection ID: {connectionId}
                                </p>
                            </div>
                        </div>

                        <div className="pipeline-form-grid">
                            <div className="form-field form-field-full">
                                <label>Source SQL (SELECT/WITH)</label>
                                <textarea
                                    className="optimizer-sql-input"
                                    value={sql}
                                    onChange={(event) => setSql(event.target.value)}
                                    placeholder="Example: SELECT * FROM src.brands"
                                />
                            </div>
                        </div>

                        <div className="pipeline-actions-row" style={{ marginTop: 14 }}>
                            <button
                                type="button"
                                className="pipeline-secondary-btn"
                                onClick={handleQueryColumns}
                                disabled={loadingColumns}
                            >
                                {loadingColumns ? "Reading..." : "Query Columns"}
                            </button>

                            <button
                                type="button"
                                className="pipeline-secondary-btn"
                                onClick={handleInferPk}
                                disabled={loadingPk}
                            >
                                {loadingPk ? "Inferring..." : "Infer Primary Keys"}
                            </button>

                            <button
                                type="button"
                                className="pipeline-primary-btn"
                                onClick={handleOptimize}
                                disabled={loadingOptimize}
                            >
                                {loadingOptimize ? "Optimizing..." : "Optimize Query"}
                            </button>

                            <button
                                type="button"
                                className="pipeline-secondary-btn"
                                onClick={handleBenchmark}
                                disabled={loadingBenchmark || !optimizedSql}
                            >
                                {loadingBenchmark ? "Benchmarking..." : "Benchmark"}
                            </button>
                        </div>
                    </div>

                    <div className="pipeline-main-card envtools-card">
                        <div className="pipeline-card-header">
                            <div>
                                <h3>PK Selection</h3>
                                <p className="pipeline-card-subtitle">
                                    Toggle columns to define dedup partition key.
                                </p>
                            </div>
                        </div>

                        {!columns.length ? (
                            <div className="pipeline-empty">Run "Query Columns" or "Infer Primary Keys" first.</div>
                        ) : (
                            <>
                                <div className="optimizer-chip-list">
                                    {columns.map((column) => (
                                        <button
                                            key={column}
                                            type="button"
                                            className={`optimizer-chip ${selectedPkCols.includes(column) ? "selected" : ""}`}
                                            onClick={() => togglePk(column)}
                                        >
                                            {column}
                                        </button>
                                    ))}
                                </div>

                                <div className="optimizer-pill-wrap">
                                    <span className="envtools-status neutral">
                                        Selected PK: {selectedPkLabel || "None"}
                                    </span>
                                </div>

                                {pkCandidates.length ? (
                                    <div className="optimizer-candidate-box">
                                        {pkCandidates.map((candidate, index) => {
                                            const candidateCols = candidate.columns || [];
                                            const candidateLabel = candidateCols.join(", ");
                                            return (
                                                <button
                                                    key={`${index}-${candidateLabel}`}
                                                    type="button"
                                                    className="optimizer-candidate-btn"
                                                    onClick={() => setSelectedPkCols(candidateCols)}
                                                    title={candidate.reason || ""}
                                                >
                                                    {`#${index + 1} ${candidateLabel || "N/A"} (conf ${Number(candidate.confidence || 0).toFixed(2)})`}
                                                </button>
                                            );
                                        })}
                                    </div>
                                ) : null}
                            </>
                        )}
                    </div>

                    <div className="pipeline-main-card envtools-card">
                        <div className="pipeline-card-header">
                            <div>
                                <h3>Dedup + Insert Settings</h3>
                                <p className="pipeline-card-subtitle">
                                    Generate PostgreSQL-safe dedup SQL and optional idempotent insert SQL.
                                </p>
                            </div>
                        </div>

                        <div className="pipeline-form-grid optimizer-grid-4">
                            <div className="form-field">
                                <label>Dedup Strategy</label>
                                <select value={dedupStrategy} onChange={(event) => setDedupStrategy(event.target.value)}>
                                    <option value="LATEST_PER_PK">LATEST_PER_PK</option>
                                    <option value="LATEST_PER_PK_WITH_HASH_ALL">LATEST_PER_PK_WITH_HASH_ALL</option>
                                    <option value="LATEST_PER_PK_WITH_HASH_EXCLUDE">LATEST_PER_PK_WITH_HASH_EXCLUDE</option>
                                </select>
                            </div>

                            <div className="form-field">
                                <label>Hash Exclude Columns (comma)</label>
                                <input
                                    value={hashExclude}
                                    onChange={(event) => setHashExclude(event.target.value)}
                                    placeholder="col1, col2"
                                />
                            </div>

                            <div className="form-field">
                                <label>Hash Column Name</label>
                                <input
                                    value={hashColName}
                                    onChange={(event) => setHashColName(event.target.value)}
                                    placeholder="ROW_HASH"
                                />
                            </div>

                            <div className="form-field">
                                <label>Expose Hash</label>
                                <select value={String(exposeHash)} onChange={(event) => setExposeHash(event.target.value === "true")}>
                                    <option value="false">false</option>
                                    <option value="true">true</option>
                                </select>
                            </div>

                            <div className="form-field">
                                <label>Target Schema (optional)</label>
                                <input
                                    value={targetSchema}
                                    onChange={(event) => setTargetSchema(event.target.value)}
                                    placeholder="core"
                                />
                            </div>

                            <div className="form-field">
                                <label>Target Table (optional)</label>
                                <input
                                    value={targetTable}
                                    onChange={(event) => setTargetTable(event.target.value)}
                                    placeholder="brands_s"
                                />
                            </div>
                        </div>

                        <div className="pipeline-actions-row" style={{ marginTop: 14 }}>
                            <button
                                type="button"
                                className="pipeline-primary-btn"
                                onClick={handleDedup}
                                disabled={loadingDedup}
                            >
                                {loadingDedup ? "Generating..." : "Generate Dedup SQL"}
                            </button>
                            <button
                                type="button"
                                className="pipeline-secondary-btn"
                                onClick={async () => copyToClipboard(dedupSql)}
                                disabled={!dedupSql}
                            >
                                Copy Dedup SQL
                            </button>
                            <button
                                type="button"
                                className="pipeline-secondary-btn"
                                onClick={async () => copyToClipboard(insertSql)}
                                disabled={!insertSql}
                            >
                                Copy Insert SQL
                            </button>
                        </div>
                    </div>

                    <div className="pipeline-split-grid">
                        <div className="pipeline-main-card envtools-card">
                            <div className="pipeline-card-header">
                                <div>
                                    <h3>Generated SQL</h3>
                                    <p className="pipeline-card-subtitle">Dedup select and rerun-safe insert statement.</p>
                                </div>
                            </div>

                            <div className="optimizer-code-stack">
                                <div>
                                    <div className="pipeline-summary-label">Dedup SQL</div>
                                    <pre className="pipeline-code-block envtools-code">{dedupSql || "No dedup SQL yet."}</pre>
                                </div>
                                <div>
                                    <div className="pipeline-summary-label">Insert SQL</div>
                                    <pre className="pipeline-code-block envtools-code">{insertSql || "Provide target schema/table to generate insert SQL."}</pre>
                                </div>
                            </div>
                        </div>

                        <div className="pipeline-main-card envtools-card">
                            <div className="pipeline-card-header">
                                <div>
                                    <h3>Validation Checks</h3>
                                    <p className="pipeline-card-subtitle">Duplicate-group counts from source and dedup output.</p>
                                </div>
                            </div>

                            {!dedupChecks ? (
                                <div className="pipeline-empty">Generate dedup SQL to view validation checks.</div>
                            ) : (
                                <div className="envtools-detail-stack">
                                    <div>
                                        <span className={`envtools-status ${dedupChecks.isDedupValid ? "success" : "error"}`}>
                                            {dedupChecks.isDedupValid ? "Dedup Valid" : "Dedup Needs Review"}
                                        </span>
                                    </div>

                                    <div className="optimizer-check-grid">
                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Source Rows</div>
                                            <div className="pipeline-summary-value-sm">{dedupChecks.sourceRowCount}</div>
                                        </div>

                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Source Duplicate Groups</div>
                                            <div className="pipeline-summary-value-sm">{dedupChecks.sourceDuplicateGroups}</div>
                                        </div>

                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Dedup Rows</div>
                                            <div className="pipeline-summary-value-sm">{dedupChecks.dedupRowCount}</div>
                                        </div>

                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Dedup Duplicate Groups</div>
                                            <div className="pipeline-summary-value-sm">{dedupChecks.dedupDuplicateGroups}</div>
                                        </div>

                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Target PK Overlap</div>
                                            <div className="pipeline-summary-value-sm">
                                                {dedupChecks.targetOverlapCount ?? "N/A"}
                                            </div>
                                        </div>

                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Would Insert</div>
                                            <div className="pipeline-summary-value-sm">
                                                {dedupChecks.wouldInsertCount ?? "N/A"}
                                            </div>
                                        </div>
                                    </div>

                                    <div>
                                        <div className="pipeline-summary-label">Source Duplicate Check SQL</div>
                                        <pre className="pipeline-code-block envtools-code">
                                            {dedupChecks.sourceDuplicateCheckSql || "N/A"}
                                        </pre>
                                    </div>

                                    <div>
                                        <div className="pipeline-summary-label">Dedup Duplicate Check SQL</div>
                                        <pre className="pipeline-code-block envtools-code">
                                            {dedupChecks.dedupDuplicateCheckSql || "N/A"}
                                        </pre>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>

                    <div className="pipeline-main-card envtools-card">
                        <div className="pipeline-card-header">
                            <div>
                                <h3>Optimizer Output</h3>
                                <p className="pipeline-card-subtitle">PostgreSQL rewrite with exact-result validation checks.</p>
                            </div>
                        </div>

                        <pre className="pipeline-code-block envtools-code">{optimizedSql || "No optimized SQL generated yet."}</pre>

                        {optExplain ? (
                            <div className="optimizer-explain-columns">
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Estimated Effect</div>
                                    <div className="pipeline-summary-value-sm">{optExplain.estimated_perf_effect || "N/A"}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Changes</div>
                                    <div className="pipeline-summary-value-sm">{(optExplain.changes || []).join(" | ") || "None"}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Notes</div>
                                    <div className="pipeline-summary-value-sm">{(optExplain.notes || []).join(" | ") || "None"}</div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Column Parity</div>
                                    <div className="pipeline-summary-value-sm">
                                        {optExplain.validation?.columnParity === undefined
                                            ? "N/A"
                                            : (optExplain.validation?.columnParity ? "Yes" : "No")}
                                    </div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Exact Data Match</div>
                                    <div className="pipeline-summary-value-sm">
                                        {optExplain.validation?.isSameData === undefined
                                            ? "N/A"
                                            : (optExplain.validation?.isSameData ? "Yes" : "No")}
                                    </div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Orig Minus Opt</div>
                                    <div className="pipeline-summary-value-sm">
                                        {optExplain.validation?.originalMinusOptimized ?? "N/A"}
                                    </div>
                                </div>
                                <div className="pipeline-summary-box">
                                    <div className="pipeline-summary-label">Opt Minus Orig</div>
                                    <div className="pipeline-summary-value-sm">
                                        {optExplain.validation?.optimizedMinusOriginal ?? "N/A"}
                                    </div>
                                </div>
                            </div>
                        ) : null}

                        {benchmark ? (
                            <div className="optimizer-benchmark-grid">
                                <span className="envtools-status neutral">Original ms: {benchmark.original?.timeMs ?? "-"}</span>
                                <span className="envtools-status neutral">Optimized ms: {benchmark.optimized?.timeMs ?? "-"}</span>
                                <span className={`envtools-status ${benchmark.diff?.isSameData ? "success" : "warning"}`}>
                                    Same Data: {benchmark.diff?.isSameData ? "Yes" : "No"}
                                </span>
                                <span className="envtools-status neutral">Orig Count: {benchmark.original?.rowCount ?? "-"}</span>
                                <span className="envtools-status neutral">Opt Count: {benchmark.optimized?.rowCount ?? "-"}</span>
                            </div>
                        ) : null}
                    </div>
                </>
            )}
        </div>
    );
}
