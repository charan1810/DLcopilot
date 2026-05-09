import { useEffect, useMemo, useState } from "react";
import {
    fetchDefinition,
    fetchObjectDetails,
    fetchObjects,
    fetchSchemas,
} from "../api/schemaApi";
import { analyzeDiffs, syncSchemaZip } from "../api/envToolsApi";
import { useAppContext } from "../context/AppContext";
import "./EnvComparator.css";

function normalizeSql(sql) {
    return String(sql || "")
        .replace(/\s+/g, " ")
        .trim()
        .toUpperCase();
}

function buildTableDefinition(schemaName, objectName, columns = []) {
    const lines = (columns || []).map((col) => {
        const nullable = String(col?.is_nullable || "YES").toUpperCase() === "NO" ? " NOT NULL" : "";
        return `  "${col.column_name}" ${col.data_type}${nullable}`;
    });

    return `CREATE TABLE "${schemaName}"."${objectName}" (\n${lines.join(",\n")}\n);`;
}

function statusClass(statusType = "") {
    const normalized = String(statusType).toLowerCase();
    if (normalized.includes("in sync")) return "success";
    if (normalized.includes("missing")) return "error";
    if (normalized.includes("logic")) return "warning";
    if (normalized.includes("column")) return "warning";
    return "neutral";
}

function downloadBlob(filename, blob) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    URL.revokeObjectURL(url);
}

function safeString(value) {
    if (value === null || value === undefined) return "";
    if (Array.isArray(value)) return value.join(" | ");
    return String(value);
}

function downloadCsv(filename, rows) {
    const header = ["Object", "Type", "Status", "Impact", "MissingInTarget", "MissingInSource"];
    const lines = [header.join(",")];

    for (const row of rows) {
        const fields = [
            safeString(row.object),
            safeString(row.type),
            safeString(row.statusType),
            safeString(row.aiImpact || ""),
            safeString(row.missingInTarget || []),
            safeString(row.missingInSource || []),
        ].map((field) => `"${String(field).replace(/"/g, '""')}"`);

        lines.push(fields.join(","));
    }

    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
    downloadBlob(filename, blob);
}

async function resolveDefinition(connectionId, databaseName, schemaName, objectName, objectType, columns) {
    if (String(objectType || "").toUpperCase() === "VIEW") {
        try {
            const res = await fetchDefinition(connectionId, databaseName, schemaName, objectName);
            const definition = String(res?.definition || "").trim();
            if (definition && !definition.toLowerCase().includes("not a view")) {
                return definition;
            }
        } catch {
            // Fallback to generated structure when definition is unavailable.
        }
    }

    return buildTableDefinition(schemaName, objectName, columns);
}

export default function EnvComparator() {
    const {
        connectionId,
        connectionPayload,
        selectedDatabase,
        selectedSchema,
    } = useAppContext();

    const databaseName = selectedDatabase || connectionPayload?.database_name || "dlcopilot";

    const [schemas, setSchemas] = useState([]);
    const [sourceSchema, setSourceSchema] = useState("");
    const [targetSchema, setTargetSchema] = useState("");

    const [rows, setRows] = useState([]);
    const [selectedKey, setSelectedKey] = useState("");
    const [search, setSearch] = useState("");

    const [loadingSchemas, setLoadingSchemas] = useState(false);
    const [loadingCompare, setLoadingCompare] = useState(false);
    const [loadingSync, setLoadingSync] = useState(false);

    const [error, setError] = useState("");
    const [info, setInfo] = useState("");

    const hasConnectionContext = !!connectionId && !!databaseName;

    useEffect(() => {
        let cancelled = false;

        async function loadSchemas() {
            if (!hasConnectionContext) {
                setSchemas([]);
                setSourceSchema("");
                setTargetSchema("");
                return;
            }

            setLoadingSchemas(true);
            setError("");

            try {
                const response = await fetchSchemas(connectionId, databaseName);
                const schemaNames = response?.schemas || [];
                if (cancelled) return;

                setSchemas(schemaNames);

                const preferredSource =
                    schemaNames.find((item) => item === selectedSchema)
                    || schemaNames.find((item) => item.toLowerCase() === "src")
                    || schemaNames[0]
                    || "";

                const preferredTarget =
                    schemaNames.find((item) => item.toLowerCase() === "core")
                    || schemaNames.find((item) => item !== preferredSource)
                    || preferredSource
                    || "";

                setSourceSchema((prev) => (prev && schemaNames.includes(prev) ? prev : preferredSource));
                setTargetSchema((prev) => (prev && schemaNames.includes(prev) ? prev : preferredTarget));
            } catch (err) {
                if (!cancelled) {
                    setError(err?.message || "Failed to load schemas.");
                }
            } finally {
                if (!cancelled) {
                    setLoadingSchemas(false);
                }
            }
        }

        loadSchemas();

        return () => {
            cancelled = true;
        };
    }, [connectionId, databaseName, hasConnectionContext, selectedSchema]);

    const compareRows = useMemo(() => {
        const q = search.trim().toLowerCase();
        const filtered = q
            ? rows.filter((row) => {
                const buffer = [
                    row.object,
                    row.type,
                    row.statusType,
                    row.aiImpact,
                    ...(row.aiComments || []),
                    ...(row.missingInTarget || []),
                    ...(row.missingInSource || []),
                ]
                    .join(" ")
                    .toLowerCase();
                return buffer.includes(q);
            })
            : rows;

        return filtered.filter((row) => String(row.statusType || "").toLowerCase() !== "in sync");
    }, [rows, search]);

    const selectedRow = useMemo(
        () => compareRows.find((row) => row.key === selectedKey) || null,
        [compareRows, selectedKey]
    );

    useEffect(() => {
        if (selectedKey && !compareRows.some((row) => row.key === selectedKey)) {
            setSelectedKey("");
        }
    }, [compareRows, selectedKey]);

    const runCompare = async () => {
        if (!hasConnectionContext) {
            setError("Save and select a PostgreSQL connection first from Explorer.");
            return;
        }
        if (!sourceSchema || !targetSchema) {
            setError("Choose both source and target schemas.");
            return;
        }

        setLoadingCompare(true);
        setError("");
        setInfo("");
        setRows([]);
        setSelectedKey("");

        try {
            const [sourceObjectsRes, targetObjectsRes] = await Promise.all([
                fetchObjects(connectionId, databaseName, sourceSchema),
                fetchObjects(connectionId, databaseName, targetSchema),
            ]);

            const sourceObjects = sourceObjectsRes?.objects || [];
            const targetObjects = targetObjectsRes?.objects || [];

            const sourceMap = new Map(sourceObjects.map((item) => [item.name, item.type]));
            const targetMap = new Map(targetObjects.map((item) => [item.name, item.type]));

            const sourceNames = new Set(sourceMap.keys());
            const targetNames = new Set(targetMap.keys());

            const onlyInSource = [...sourceNames].filter((name) => !targetNames.has(name)).sort();
            const onlyInTarget = [...targetNames].filter((name) => !sourceNames.has(name)).sort();
            const common = [...sourceNames].filter((name) => targetNames.has(name)).sort();

            const workingRows = [];

            for (const objectName of onlyInSource) {
                workingRows.push({
                    key: objectName,
                    object: objectName,
                    type: sourceMap.get(objectName) || "OBJECT",
                    statusType: "Missing in Target",
                    aiImpact: "Medium",
                    aiComments: ["Object exists only in source schema."],
                    missingInTarget: ["<OBJECT>"] ,
                    missingInSource: [],
                });
            }

            for (const objectName of onlyInTarget) {
                workingRows.push({
                    key: objectName,
                    object: objectName,
                    type: targetMap.get(objectName) || "OBJECT",
                    statusType: "Missing in Source",
                    aiImpact: "Medium",
                    aiComments: ["Object exists only in target schema."],
                    missingInTarget: [],
                    missingInSource: ["<OBJECT>"],
                });
            }

            const detailedCommonRows = await Promise.all(
                common.map(async (objectName) => {
                    const objectType = sourceMap.get(objectName) || targetMap.get(objectName) || "OBJECT";

                    const [sourceDetails, targetDetails] = await Promise.all([
                        fetchObjectDetails(connectionId, databaseName, sourceSchema, objectName),
                        fetchObjectDetails(connectionId, databaseName, targetSchema, objectName),
                    ]);

                    const srcColumnsRaw = sourceDetails?.columns || [];
                    const tgtColumnsRaw = targetDetails?.columns || [];

                    const srcCols = srcColumnsRaw.map((col) => col.column_name);
                    const tgtCols = tgtColumnsRaw.map((col) => col.column_name);

                    const [srcDDL, tgtDDL] = await Promise.all([
                        resolveDefinition(connectionId, databaseName, sourceSchema, objectName, objectType, srcColumnsRaw),
                        resolveDefinition(connectionId, databaseName, targetSchema, objectName, objectType, tgtColumnsRaw),
                    ]);

                    return {
                        key: objectName,
                        object: objectName,
                        type: objectType,
                        statusType: "Analyzing...",
                        srcCols,
                        tgtCols,
                        srcDDL,
                        tgtDDL,
                        missingInTarget: [],
                        missingInSource: [],
                    };
                })
            );

            let analyzedRows = detailedCommonRows;
            const analysisResponse = await analyzeDiffs(
                detailedCommonRows.map((item) => ({
                    key: item.key,
                    object: `${sourceSchema}.${item.object}`,
                    type: item.type,
                    srcCols: item.srcCols,
                    tgtCols: item.tgtCols,
                    srcDDL: item.srcDDL,
                    tgtDDL: item.tgtDDL,
                }))
            );

            if (analysisResponse?.data?.success) {
                const analysisMap = analysisResponse.data.results || {};
                analyzedRows = detailedCommonRows.map((item) => {
                    const analysis = analysisMap[item.key];
                    if (!analysis) return item;

                    return {
                        ...item,
                        statusType: analysis.statusType || item.statusType,
                        aiImpact: analysis.impact || "",
                        aiComments: analysis.comments || [],
                        missingInTarget: analysis.missingInTarget || item.missingInTarget,
                        missingInSource: analysis.missingInSource || item.missingInSource,
                    };
                });
            } else {
                analyzedRows = detailedCommonRows.map((item) => {
                    const srcNorm = normalizeSql(item.srcDDL);
                    const tgtNorm = normalizeSql(item.tgtDDL);
                    const colDiff =
                        item.srcCols.length !== item.tgtCols.length
                        || item.srcCols.some((col) => !item.tgtCols.includes(col))
                        || item.tgtCols.some((col) => !item.srcCols.includes(col));

                    if (!colDiff && srcNorm === tgtNorm) {
                        return {
                            ...item,
                            statusType: "In Sync",
                            aiImpact: "Negligible",
                            aiComments: ["No significant difference detected."],
                        };
                    }

                    if (colDiff && srcNorm === tgtNorm) {
                        return {
                            ...item,
                            statusType: "Column Difference",
                            aiImpact: "Medium",
                            aiComments: ["Columns differ between schemas."],
                        };
                    }

                    return {
                        ...item,
                        statusType: "Logic Difference",
                        aiImpact: "High",
                        aiComments: ["Definition text differs between schemas."],
                    };
                });
            }

            const finalRows = [...workingRows, ...analyzedRows].sort((left, right) =>
                String(left.object || "").localeCompare(String(right.object || ""))
            );

            setRows(finalRows);
            setSelectedKey(finalRows[0]?.key || "");
            setInfo(`Compared ${sourceSchema} vs ${targetSchema} in ${databaseName}.`);
        } catch (err) {
            setError(err?.message || "Comparison failed.");
        } finally {
            setLoadingCompare(false);
        }
    };

    const downloadSyncScripts = async () => {
        const candidates = compareRows.filter((row) => {
            const status = String(row.statusType || "");
            return status === "Missing in Source" || status === "Column Difference" || status === "Logic Difference";
        });

        if (!candidates.length) {
            setInfo("No objects require sync scripts.");
            return;
        }

        setLoadingSync(true);
        setError("");

        try {
            const response = await syncSchemaZip({
                source_db: databaseName,
                source_schema: sourceSchema,
                target_db: databaseName,
                target_schema: targetSchema,
                items: candidates.map((row) => ({
                    key: row.key,
                    object: row.object,
                    type: row.type,
                    statusType: row.statusType,
                    srcDDL: row.srcDDL || "",
                    tgtDDL: row.tgtDDL || "",
                })),
            });

            const blob = response?.data instanceof Blob
                ? response.data
                : new Blob([response?.data], { type: "application/zip" });

            const filename = `sync_${sourceSchema}_from_${targetSchema}.zip`;
            downloadBlob(filename, blob);
            setInfo(`Downloaded sync scripts for ${candidates.length} object(s).`);
        } catch (err) {
            setError(err?.message || "Failed to generate sync scripts.");
        } finally {
            setLoadingSync(false);
        }
    };

    return (
        <div className="page">
            <div className="module-hero compact-hero">
                <div className="module-badge">Utilities</div>
                <h1 className="module-title">Environment Comparator</h1>
                <p className="module-subtitle">
                    PostgreSQL-first comparison for two schemas in the active saved connection.
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
                                <h3>Comparison Setup</h3>
                                <p className="pipeline-card-subtitle">
                                    Database: {databaseName} | Connection ID: {connectionId}
                                </p>
                            </div>
                        </div>

                        <div className="pipeline-form-grid">
                            <div className="form-field">
                                <label>Source Schema</label>
                                <select
                                    value={sourceSchema}
                                    onChange={(event) => setSourceSchema(event.target.value)}
                                    disabled={loadingSchemas || loadingCompare}
                                >
                                    {schemas.map((schemaName) => (
                                        <option key={schemaName} value={schemaName}>
                                            {schemaName}
                                        </option>
                                    ))}
                                </select>
                            </div>

                            <div className="form-field">
                                <label>Target Schema</label>
                                <select
                                    value={targetSchema}
                                    onChange={(event) => setTargetSchema(event.target.value)}
                                    disabled={loadingSchemas || loadingCompare}
                                >
                                    {schemas.map((schemaName) => (
                                        <option key={schemaName} value={schemaName}>
                                            {schemaName}
                                        </option>
                                    ))}
                                </select>
                            </div>

                            <div className="form-field form-field-full">
                                <label>Search Differences</label>
                                <input
                                    value={search}
                                    onChange={(event) => setSearch(event.target.value)}
                                    placeholder="Filter by object, status, comments, or columns"
                                />
                            </div>
                        </div>

                        <div className="pipeline-actions-row" style={{ marginTop: 14 }}>
                            <button
                                type="button"
                                className="pipeline-primary-btn"
                                onClick={runCompare}
                                disabled={loadingSchemas || loadingCompare || !sourceSchema || !targetSchema}
                            >
                                {loadingCompare ? "Comparing..." : "Compare Schemas"}
                            </button>

                            <button
                                type="button"
                                className="pipeline-secondary-btn"
                                onClick={downloadSyncScripts}
                                disabled={loadingSync || !compareRows.length}
                            >
                                {loadingSync ? "Preparing..." : "Download Sync Scripts"}
                            </button>

                            <button
                                type="button"
                                className="pipeline-secondary-btn"
                                onClick={() => downloadCsv("schema_comparison.csv", compareRows)}
                                disabled={!compareRows.length}
                            >
                                Export CSV
                            </button>
                        </div>
                    </div>

                    <div className="pipeline-split-grid">
                        <div className="pipeline-main-card envtools-card">
                            <div className="pipeline-card-header">
                                <div>
                                    <h3>Differences</h3>
                                    <p className="pipeline-card-subtitle">
                                        {compareRows.length} object(s) with differences
                                    </p>
                                </div>
                            </div>

                            <div className="envtools-table-wrap">
                                <table className="envtools-table">
                                    <thead>
                                        <tr>
                                            <th>Object</th>
                                            <th>Type</th>
                                            <th>Status</th>
                                            <th>Impact</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {compareRows.map((row) => (
                                            <tr
                                                key={row.key}
                                                className={selectedKey === row.key ? "selected" : ""}
                                                onClick={() => setSelectedKey(row.key)}
                                            >
                                                <td>{row.object}</td>
                                                <td>{row.type}</td>
                                                <td>
                                                    <span className={`envtools-status ${statusClass(row.statusType)}`}>
                                                        {row.statusType}
                                                    </span>
                                                </td>
                                                <td>{row.aiImpact || "-"}</td>
                                            </tr>
                                        ))}
                                        {!compareRows.length ? (
                                            <tr>
                                                <td colSpan={4} className="envtools-empty-row">
                                                    Run a comparison to view differences.
                                                </td>
                                            </tr>
                                        ) : null}
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        <div className="pipeline-main-card envtools-card">
                            <div className="pipeline-card-header">
                                <div>
                                    <h3>Object Details</h3>
                                    <p className="pipeline-card-subtitle">
                                        {selectedRow ? selectedRow.object : "Select a row from the table"}
                                    </p>
                                </div>
                            </div>

                            {!selectedRow ? (
                                <div className="pipeline-empty">Select an object to review details and SQL definitions.</div>
                            ) : (
                                <div className="envtools-detail-stack">
                                    <div>
                                        <span className={`envtools-status ${statusClass(selectedRow.statusType)}`}>
                                            {selectedRow.statusType}
                                        </span>
                                    </div>

                                    {(selectedRow.aiComments || []).length ? (
                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Notes</div>
                                            <ul className="envtools-note-list">
                                                {selectedRow.aiComments.map((comment) => (
                                                    <li key={comment}>{comment}</li>
                                                ))}
                                            </ul>
                                        </div>
                                    ) : null}

                                    <div className="envtools-two-col">
                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Missing In Target</div>
                                            <div className="pipeline-summary-value-sm">
                                                {(selectedRow.missingInTarget || []).length
                                                    ? selectedRow.missingInTarget.join(", ")
                                                    : "None"}
                                            </div>
                                        </div>

                                        <div className="pipeline-summary-box">
                                            <div className="pipeline-summary-label">Missing In Source</div>
                                            <div className="pipeline-summary-value-sm">
                                                {(selectedRow.missingInSource || []).length
                                                    ? selectedRow.missingInSource.join(", ")
                                                    : "None"}
                                            </div>
                                        </div>
                                    </div>

                                    <div className="envtools-two-col">
                                        <div>
                                            <div className="pipeline-summary-label">Source Definition ({sourceSchema})</div>
                                            <pre className="pipeline-code-block envtools-code">{selectedRow.srcDDL || "No definition."}</pre>
                                        </div>

                                        <div>
                                            <div className="pipeline-summary-label">Target Definition ({targetSchema})</div>
                                            <pre className="pipeline-code-block envtools-code">{selectedRow.tgtDDL || "No definition."}</pre>
                                        </div>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
