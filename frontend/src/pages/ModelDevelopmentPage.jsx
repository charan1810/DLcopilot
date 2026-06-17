import { useEffect, useState, useCallback } from "react";
import { BoxSelect, Plus, Pencil, Trash2, CheckCircle, X, ChevronRight, Rocket, Database, Layers, ChevronDown, ChevronRight as ChevronRightIcon } from "lucide-react";
import { fetchSavedConnections, fetchSchemas, fetchObjects, fetchObjectDetails } from "../api/schemaApi";
import {
    listModels,
    createModel,
    updateModel,
    deleteModel,
    validateModel,
    deployModel,
    createSchema,
    listModelTemplates,
    createModelTemplate,
} from "../api/modelApi";
import { useAuth } from "../context/AuthContext";
import { useAppContext } from "../context/AppContext";
import "./ModelDevelopmentPage.css";

// ── Constants ────────────────────────────────────────────────────────────────

const STEPS = [
    { id: "setup",     label: "Setup"     },
    { id: "type",      label: "Entity Type" },
    { id: "columns",   label: "Columns"   },
    { id: "rules",     label: "Bus. Rules" },
    { id: "validate",  label: "Validate"  },
    { id: "deploy",    label: "Deploy"    },
];

const STEP_INDEX = Object.fromEntries(STEPS.map((s, i) => [s.id, i]));

const DEFAULT_COL = () => ({
    name: "",
    data_type: "VARCHAR(255)",
    nullable: true,
    primary_key: false,
    unique: false,
    default_value: "",
    comment: "",
});

const COMMON_TYPES = [
    "VARCHAR(255)", "VARCHAR(512)", "TEXT", "INTEGER", "BIGINT", "SMALLINT",
    "NUMERIC(18,2)", "FLOAT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP",
    "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "VARIANT", "OBJECT", "ARRAY", "BINARY",
];

const TABLE_TYPES = [
    { value: "regular",   icon: "🗄️", label: "Regular Table",    desc: "Standard persistent table."          },
    { value: "iceberg",   icon: "🏔️", label: "Iceberg Table",    desc: "Open table format for large-scale analytics." },
    { value: "transient", icon: "⚡", label: "Transient Table",  desc: "Reduced Fail-safe; lower storage cost." },
    { value: "temporary", icon: "⏱️", label: "Temporary Table",  desc: "Session-scoped; auto-dropped on close." },
];

// ── Helpers ───────────────────────────────────────────────────────────────────

function statusBadge(status) {
    const cls = {
        draft:     "mdp-badge mdp-badge-draft",
        validated: "mdp-badge mdp-badge-validated",
        deployed:  "mdp-badge mdp-badge-deployed",
    }[status] || "mdp-badge mdp-badge-draft";
    return <span className={cls}>{status}</span>;
}

function scoreColor(score) {
    if (score >= 0.8) return "high";
    if (score >= 0.5) return "mid";
    return "low";
}

function blankDraft() {
    return {
        name: "",
        description: "",
        source_connection_id: "",
        source_schema: "",
        source_tables: [],
        target_connection_id: "",
        target_schema: "",
        object_type: "table",
        table_type: "regular",
        columns_def: [DEFAULT_COL()],
        transformations: {},
        business_rules: "",
    };
}

function connectionSignature(conn) {
    return [
        (conn?.name || "").trim().toLowerCase(),
        (conn?.db_type || "").trim().toLowerCase(),
        (conn?.host || "").trim().toLowerCase(),
        String(conn?.port || "").trim(),
        (conn?.database_name || "").trim().toLowerCase(),
        (conn?.schema_name || "").trim().toLowerCase(),
        (conn?.username || "").trim().toLowerCase(),
        (conn?.account || "").trim().toLowerCase(),
        (conn?.warehouse || "").trim().toLowerCase(),
        (conn?.role || "").trim().toLowerCase(),
    ].join("|");
}

function dedupeConnections(connections = []) {
    const seen = new Set();
    const out = [];

    // Walk backward so duplicates keep the most recently saved entry.
    for (let i = connections.length - 1; i >= 0; i -= 1) {
        const conn = connections[i];
        const sig = connectionSignature(conn);
        if (seen.has(sig)) continue;
        seen.add(sig);
        out.unshift(conn);
    }

    return out;
}

// ── Sub-components ────────────────────────────────────────────────────────────

function StepsBar({ currentStep }) {
    const idx = STEP_INDEX[currentStep] ?? 0;
    return (
        <div className="mdp-steps-bar">
            {STEPS.map((step, i) => (
                <div key={step.id} className="mdp-step-item">
                    <div className={`mdp-step-dot ${i < idx ? "done" : i === idx ? "active" : ""}`}>
                        {i < idx ? "✓" : i + 1}
                    </div>
                    <span className={`mdp-step-label ${i === idx ? "active" : ""}`}>{step.label}</span>
                    {i < STEPS.length - 1 && <div className="mdp-step-sep" />}
                </div>
            ))}
        </div>
    );
}

function ColumnBuilder({ columns, onChange }) {
    const [showBulkPaste, setShowBulkPaste] = useState(false);
    const [bulkText, setBulkText] = useState("");
    const [bulkStatus, setBulkStatus] = useState("");

    const parseBool = (token, defaultValue) => {
        if (token == null) return defaultValue;
        const normalized = String(token).trim().toLowerCase();
        if (!normalized) return defaultValue;

        if (["true", "t", "yes", "y", "1", "nullable", "null"].includes(normalized)) return true;
        if (["false", "f", "no", "n", "0", "not null", "not_null", "nn", "required"].includes(normalized)) return false;

        return defaultValue;
    };

    const parsePk = (token) => {
        if (token == null) return false;
        const normalized = String(token).trim().toLowerCase();
        return ["true", "t", "yes", "y", "1", "pk", "primary", "primary_key"].includes(normalized);
    };

    const parseUnique = (token) => {
        if (token == null) return false;
        const normalized = String(token).trim().toLowerCase();
        return ["true", "t", "yes", "y", "1", "unique", "uq"].includes(normalized);
    };

    const DATA_TYPE_RE = /^(varchar|char|text|integer|int|bigint|smallint|numeric|decimal|float|double|boolean|bool|date|timestamp|timestamp_ntz|timestamp_tz|variant|object|array|binary|serial|bigserial|json|jsonb|uuid)\b/i;

    const parseBulkColumns = (text) => {
        const lines = String(text || "").split(/\r?\n/);
        const parsed = [];
        let skipped = 0;

        for (const rawLine of lines) {
            const line = rawLine.trim();
            if (!line || line.startsWith("#")) continue;

            let parts;
            if (line.includes("\t")) {
                parts = line.split("\t");
            } else if (line.includes(",")) {
                parts = line.split(",");
            } else if (line.includes("|")) {
                parts = line.split("|");
            } else {
                parts = [line];
            }

            const clean = parts.map((p) => String(p || "").trim()).filter((p, idx) => p || idx === 0);

            // If the second token doesn't look like a data type, treat every
            // token as its own column name (all default to VARCHAR(255)).
            const secondLooksLikeType = clean.length > 1 && DATA_TYPE_RE.test(clean[1]);
            if (!secondLooksLikeType && clean.length > 1) {
                for (const token of clean) {
                    const rawName = token.replace(/^[`"']+|[`"']+$/g, "").trim();
                    if (!rawName) { skipped += 1; continue; }
                    parsed.push({ ...DEFAULT_COL(), name: rawName });
                }
                continue;
            }

            const rawName = String(clean[0] || "").replace(/^[`"']+|[`"']+$/g, "").trim();
            if (!rawName) {
                skipped += 1;
                continue;
            }

            const nextCol = {
                ...DEFAULT_COL(),
                name: rawName,
            };

            if (clean[1]) nextCol.data_type = clean[1];
            if (clean.length > 2) nextCol.nullable = parseBool(clean[2], nextCol.nullable);
            if (clean.length > 3) nextCol.primary_key = parsePk(clean[3]);
            if (clean.length > 4) nextCol.unique = parseUnique(clean[4]);

            parsed.push(nextCol);
        }

        return { parsed, skipped };
    };

    const applyBulkColumns = (mode = "append") => {
        const currentCols = Array.isArray(columns) ? columns : [];
        const { parsed, skipped } = parseBulkColumns(bulkText);
        let skippedCount = skipped;
        if (!parsed.length) {
            setBulkStatus("No valid columns found in pasted text.");
            return;
        }

        const base = mode === "replace" ? [] : [...currentCols];
        const seen = new Set(base.map((c) => String(c?.name || "").trim().toLowerCase()).filter(Boolean));
        const toAdd = [];
        let duplicates = 0;

        for (const col of parsed) {
            const key = String(col.name || "").trim().toLowerCase();
            if (!key) {
                skippedCount += 1;
                continue;
            }
            if (seen.has(key)) {
                duplicates += 1;
                continue;
            }
            seen.add(key);
            toAdd.push(col);
        }

        const next = [...base, ...toAdd];
        onChange(next.length ? next : [DEFAULT_COL()]);
        setBulkStatus(`Imported ${toAdd.length} column(s). Skipped ${duplicates + skippedCount}.`);
    };

    const update = (i, field, value) => {
        const next = columns.map((c, idx) => idx === i ? { ...c, [field]: value } : c);
        onChange(next);
    };
    const addRow = () => onChange([...columns, DEFAULT_COL()]);
    const removeRow = (i) => onChange(columns.filter((_, idx) => idx !== i));

    return (
        <div>
            <div className="mdp-col-list">
                {/* Header */}
                <div className="mdp-col-row mdp-col-row-header">
                    <span>Column Name</span>
                    <span>Data Type</span>
                    <span style={{ textAlign: "center" }}>Nullable</span>
                    <span style={{ textAlign: "center" }}>PK</span>
                    <span />
                </div>

                {columns.map((col, i) => (
                    <div key={i} className="mdp-col-row">
                        <input
                            className="mdp-col-input"
                            placeholder="column_name"
                            value={col.name}
                            onChange={(e) => update(i, "name", e.target.value)}
                        />
                        <select
                            className="mdp-col-input"
                            value={col.data_type}
                            onChange={(e) => update(i, "data_type", e.target.value)}
                        >
                            {COMMON_TYPES.map((t) => <option key={t}>{t}</option>)}
                        </select>
                        <div className="mdp-col-toggle">
                            <input
                                type="checkbox"
                                checked={col.nullable}
                                onChange={(e) => update(i, "nullable", e.target.checked)}
                                title="Nullable"
                            />
                        </div>
                        <div className="mdp-col-toggle">
                            <input
                                type="checkbox"
                                checked={col.primary_key}
                                onChange={(e) => update(i, "primary_key", e.target.checked)}
                                title="Primary Key"
                            />
                        </div>
                        <button className="mdp-col-remove" onClick={() => removeRow(i)} title="Remove">✕</button>
                    </div>
                ))}
            </div>

            <div className="mdp-col-actions">
                <button className="btn-secondary" style={{ fontSize: 12 }} onClick={addRow}>
                    + Add Column
                </button>
                <button
                    className="btn-secondary"
                    style={{ fontSize: 12 }}
                    onClick={() => {
                        setShowBulkPaste((v) => !v);
                        setBulkStatus("");
                    }}
                >
                    {showBulkPaste ? "Hide Paste Box" : "Paste Columns"}
                </button>
            </div>

            {showBulkPaste && (
                <div className="mdp-col-bulk-panel">
                    <div className="mdp-col-bulk-help">
                        Paste one column per line, or CSV/TSV rows in this format:
                        <br />
                        name,data_type,nullable,primary_key,unique
                    </div>
                    <textarea
                        className="mdp-col-bulk-textarea"
                        placeholder={"order_id\norder_number\norder_date\nnet_amount,NUMERIC(18,2),false,false,false"}
                        value={bulkText}
                        onChange={(e) => {
                            setBulkText(e.target.value);
                            if (bulkStatus) setBulkStatus("");
                        }}
                    />
                    <div className="mdp-col-bulk-buttons">
                        <button className="btn-secondary" style={{ fontSize: 12 }} onClick={() => applyBulkColumns("append")}>
                            Append Parsed Columns
                        </button>
                        <button className="btn-primary" style={{ fontSize: 12 }} onClick={() => applyBulkColumns("replace")}>
                            Replace With Parsed Columns
                        </button>
                        <button
                            className="btn-secondary"
                            style={{ fontSize: 12 }}
                            onClick={() => {
                                setBulkText("");
                                setBulkStatus("");
                            }}
                        >
                            Clear
                        </button>
                    </div>
                    {bulkStatus && <div className="mdp-col-bulk-status">{bulkStatus}</div>}
                </div>
            )}
        </div>
    );
}

// ── Wizard Panel ──────────────────────────────────────────────────────────────

function WizardPanel({ draft, connections, models, onClose, onSaved, onBulkCreated, editingId }) {
    const [step, setStep]           = useState("setup");
    const [form, setForm]           = useState(() => ({ ...blankDraft(), ...draft }));
    const [loading, setLoading]     = useState(false);
    const [error, setError]         = useState(null);
    const [savedId, setSavedId]     = useState(editingId || null);
    const [validated, setValidated] = useState(null);  // model after AI validation
    const [deployed, setDeployed]   = useState(null);  // model after deploy

    // Deploy step target schema state
    const [targetSchemas, setTargetSchemas] = useState([]);
    const [targetSchemasLoading, setTargetSchemasLoading] = useState(false);
    const [showCreateSchema, setShowCreateSchema] = useState(false);
    const [newSchemaName, setNewSchemaName] = useState("");
    const [createSchemaLoading, setCreateSchemaLoading] = useState(false);
    const [createSchemaError, setCreateSchemaError] = useState("");

    const [templates, setTemplates] = useState([]);
    const [selectedTemplateId, setSelectedTemplateId] = useState("");
    const [showTemplateModal, setShowTemplateModal] = useState(false);
    const [templateName, setTemplateName] = useState("");
    const [templateDescription, setTemplateDescription] = useState("");
    const [templateBusy, setTemplateBusy] = useState(false);
    const [templateError, setTemplateError] = useState("");
    const [bulkBusy, setBulkBusy] = useState(false);
    const [bulkResult, setBulkResult] = useState("");
    const [bulkNamesText, setBulkNamesText] = useState("");
    const [selectedRefModelId, setSelectedRefModelId] = useState("");
    const [templateMode, setTemplateMode] = useState("custom"); // custom | source
    const [sourceTemplateSchemas, setSourceTemplateSchemas] = useState([]);
    const [sourceTemplateObjects, setSourceTemplateObjects] = useState([]);
    const [sourceTemplateSchemasLoading, setSourceTemplateSchemasLoading] = useState(false);
    const [sourceTemplateObjectsLoading, setSourceTemplateObjectsLoading] = useState(false);
    const [sourceTemplateBusy, setSourceTemplateBusy] = useState(false);

    const [bulkTemplateMode, setBulkTemplateMode] = useState("custom"); // custom | source
    const [bulkSelectedTemplateId, setBulkSelectedTemplateId] = useState("");
    const [bulkSelectedRefModelId, setBulkSelectedRefModelId] = useState("");

    const { user } = useAuth();
    const {
        sessionPassword,
        connectionId: explorerConnectionId,
        selectedDatabase: explorerDatabase,
        setExplorerCache,
    } = useAppContext();
    const canManageSchemas = user?.role === "admin" || user?.role === "architect";

    const set = (field, value) => setForm((f) => ({ ...f, [field]: value }));

    const loadTemplates = useCallback(async () => {
        try {
            const list = await listModelTemplates();
            setTemplates(list || []);
        } catch {
            setTemplates([]);
        }
    }, []);

    useEffect(() => {
        loadTemplates();
    }, [loadTemplates]);

    const selectedTargetConnection = connections.find((c) => String(c.id) === String(form.target_connection_id));
    const targetDatabaseName = selectedTargetConnection?.database_name || "postgres";
    const sourceTemplateConnectionId = Number(form.target_connection_id || 0);
    const selectedSourceTemplateConnection = connections.find((c) => String(c.id) === String(sourceTemplateConnectionId));
    const sourceTemplateDatabaseName = selectedSourceTemplateConnection?.database_name || "postgres";
    const resolvedBulkSourceConnectionId = Number(form.target_connection_id || 0);

    const saveTeamTemplate = async () => {
        const name = templateName.trim();
        if (!name) {
            setTemplateError("Template name is required.");
            return;
        }

        const cols = (form.columns_def || []).filter((c) => c.name?.trim());
        if (!cols.length) {
            setTemplateError("No columns to save. Define columns first, then save as template.");
            return;
        }

        setTemplateBusy(true);
        setTemplateError("");
        try {
            await createModelTemplate({
                name,
                description: templateDescription || null,
                source_schema: form.source_schema || null,
                target_schema_default: form.target_schema || null,
                object_type: form.object_type || "table",
                table_type: form.table_type || null,
                columns_def: cols,
                transformations: form.transformations || null,
                business_rules: form.business_rules || null,
            });
            await loadTemplates();
            setTemplateName("");
            setTemplateDescription("");
            setShowTemplateModal(false);
        } catch (err) {
            setTemplateError(err.message || "Failed to save template");
        } finally {
            setTemplateBusy(false);
        }
    };

    const runBulkCreateByNames = async () => {
        const lines = bulkNamesText
            .split(/\r?\n/)
            .map((n) => n.trim())
            .filter(Boolean);

        if (!lines.length) {
            setError("Enter at least one entity name.");
            return;
        }
        if (!form.target_connection_id || !form.target_schema) {
            setError("Select target connection and schema before bulk create.");
            return;
        }

        const parseBulkSourceLine = (line) => {
            const parts = line.includes("\t")
                ? line.split("\t")
                : line.includes("|")
                    ? line.split("|")
                    : line.split(",");
            return parts.map((p) => String(p || "").trim()).filter(Boolean);
        };

        // Resolve columns and settings from template or reference entity
        let columns_def = [DEFAULT_COL()];
        let object_type = "table";
        let table_type = "regular";
        let business_rules = "";

        if (bulkSelectedTemplateId) {
            const tmpl = templates.find((t) => String(t.id) === String(bulkSelectedTemplateId));
            if (tmpl) {
                columns_def = tmpl.columns_def || [DEFAULT_COL()];
                object_type = tmpl.object_type || "table";
                table_type = tmpl.table_type || "regular";
                business_rules = tmpl.business_rules || "";
            }
        } else if (bulkSelectedRefModelId) {
            const refModel = (models || []).find((m) => String(m.id) === String(bulkSelectedRefModelId));
            if (refModel) {
                columns_def = refModel.columns_def || [DEFAULT_COL()];
                object_type = refModel.object_type || "table";
                table_type = refModel.table_type || "regular";
                business_rules = refModel.business_rules || "";
            }
        }

        setBulkBusy(true);
        setBulkResult("");
        setError(null);
        let created = 0;
        let skipped = 0;

        if (bulkTemplateMode === "source") {
            for (const line of lines) {
                const parsed = parseBulkSourceLine(line);
                if (parsed.length < 3) {
                    skipped += 1;
                    continue;
                }

                const [targetName, srcSchema, srcObject] = parsed;
                const exists = (models || []).some((m) => m.name === targetName);
                if (!targetName || !srcSchema || !srcObject || exists) {
                    skipped += 1;
                    continue;
                }

                try {
                    const details = await fetchObjectDetails(
                        resolvedBulkSourceConnectionId,
                        targetDatabaseName,
                        srcSchema,
                        srcObject,
                    );

                    const mappedColumns = (details?.columns || []).map((col) => ({
                        ...DEFAULT_COL(),
                        name: String(col.column_name || "").trim(),
                        data_type: String(col.data_type || "VARCHAR(255)").toUpperCase(),
                        nullable: String(col.is_nullable || "YES").toUpperCase() !== "NO",
                    })).filter((c) => c.name);

                    if (!mappedColumns.length) {
                        skipped += 1;
                        continue;
                    }

                    const mappedObjectType = String(details?.object_type || "TABLE").toUpperCase() === "VIEW" ? "view" : "table";

                    await createModel({
                        name: targetName,
                        description: "",
                        source_connection_id: resolvedBulkSourceConnectionId,
                        source_schema: srcSchema,
                        source_tables: [srcObject],
                        target_connection_id: Number(form.target_connection_id),
                        target_schema: form.target_schema,
                        object_type: mappedObjectType,
                        table_type: mappedObjectType === "table" ? "regular" : null,
                        columns_def: mappedColumns,
                        transformations: null,
                        business_rules: "",
                    });
                    created += 1;
                } catch {
                    skipped += 1;
                }
            }
        } else {
            for (const name of lines) {
                const exists = (models || []).some((m) => m.name === name);
                if (exists) { skipped += 1; continue; }
                try {
                    await createModel({
                        name,
                        description: "",
                        source_connection_id: null,
                        source_schema: null,
                        source_tables: [],
                        target_connection_id: Number(form.target_connection_id),
                        target_schema: form.target_schema,
                        object_type,
                        table_type: object_type === "table" ? table_type : null,
                        columns_def,
                        transformations: null,
                        business_rules,
                    });
                    created += 1;
                } catch {
                    skipped += 1;
                }
            }
        }

        setBulkBusy(false);
        setBulkResult(`Created ${created} entity(ies), skipped ${skipped}.`);
        if (created > 0 && onBulkCreated) onBulkCreated();
    };

    useEffect(() => {
        if (templateMode !== "source") {
            setSourceTemplateSchemas([]);
            setSourceTemplateObjects([]);
            return;
        }
        if (!Number.isInteger(sourceTemplateConnectionId) || sourceTemplateConnectionId <= 0) {
            setSourceTemplateSchemas([]);
            setSourceTemplateObjects([]);
            return;
        }

        let active = true;
        setSourceTemplateSchemasLoading(true);
        (async () => {
            try {
                const data = await fetchSchemas(sourceTemplateConnectionId, sourceTemplateDatabaseName);
                if (!active) return;
                setSourceTemplateSchemas(data?.schemas || []);
            } catch {
                if (!active) return;
                setSourceTemplateSchemas([]);
            } finally {
                if (active) setSourceTemplateSchemasLoading(false);
            }
        })();

        return () => { active = false; };
    }, [templateMode, sourceTemplateConnectionId, sourceTemplateDatabaseName]);

    useEffect(() => {
        if (templateMode !== "source") {
            setSourceTemplateObjects([]);
            return;
        }
        if (!Number.isInteger(sourceTemplateConnectionId) || sourceTemplateConnectionId <= 0 || !form.source_schema) {
            setSourceTemplateObjects([]);
            return;
        }

        let active = true;
        setSourceTemplateObjectsLoading(true);
        (async () => {
            try {
                const data = await fetchObjects(sourceTemplateConnectionId, sourceTemplateDatabaseName, form.source_schema);
                if (!active) return;
                setSourceTemplateObjects(data?.objects || []);
            } catch {
                if (!active) return;
                setSourceTemplateObjects([]);
            } finally {
                if (active) setSourceTemplateObjectsLoading(false);
            }
        })();

        return () => { active = false; };
    }, [templateMode, sourceTemplateConnectionId, sourceTemplateDatabaseName, form.source_schema]);

    const applySourceObjectTemplate = async () => {
        const sourceObjectName = form.source_tables?.[0] || "";
        if (!sourceTemplateConnectionId || !form.source_schema || !sourceObjectName) {
            setError("Select source schema and object first.");
            return;
        }

        setSourceTemplateBusy(true);
        setError(null);
        try {
            const details = await fetchObjectDetails(
                sourceTemplateConnectionId,
                sourceTemplateDatabaseName,
                form.source_schema,
                sourceObjectName,
            );

            const sourceCols = (details?.columns || []).map((col) => ({
                ...DEFAULT_COL(),
                name: String(col.column_name || "").trim(),
                data_type: String(col.data_type || "VARCHAR(255)").toUpperCase(),
                nullable: String(col.is_nullable || "YES").toUpperCase() !== "NO",
            })).filter((c) => c.name);

            if (!sourceCols.length) {
                setError("Selected source object has no readable columns.");
                return;
            }

            setForm((prev) => ({
                ...prev,
                source_connection_id: String(sourceTemplateConnectionId),
                source_schema: form.source_schema,
                source_tables: [sourceObjectName],
                object_type: String(details?.object_type || "TABLE").toUpperCase() === "VIEW" ? "view" : "table",
                table_type: String(details?.object_type || "TABLE").toUpperCase() === "VIEW" ? prev.table_type : "regular",
                columns_def: sourceCols,
            }));

            setSelectedTemplateId("");
            setSelectedRefModelId("");
        } catch (err) {
            setError(err.message || "Failed to apply source object template.");
        } finally {
            setSourceTemplateBusy(false);
        }
    };

    useEffect(() => {
        if (templateMode !== "source") return;
        const selectedObject = form.source_tables?.[0] || "";
        if (!selectedObject || !form.source_schema || !sourceTemplateConnectionId) return;
        applySourceObjectTemplate();
        // Intentionally react only to concrete source selectors.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [templateMode, form.source_schema, form.source_tables?.[0], sourceTemplateConnectionId]);

    const applySingleTemplateSelection = () => {
        if (templateMode !== "custom") return;

        if (selectedTemplateId) {
            const tmpl = templates.find((t) => String(t.id) === String(selectedTemplateId));
            if (!tmpl) {
                setError("Selected template was not found.");
                return;
            }

            setForm((prev) => ({
                ...prev,
                columns_def: (tmpl.columns_def || [DEFAULT_COL()]).map((c) => ({ ...DEFAULT_COL(), ...c })),
                object_type: tmpl.object_type || "table",
                table_type: tmpl.table_type || "regular",
                business_rules: tmpl.business_rules || prev.business_rules,
            }));
            setError(null);
            return;
        }

        if (selectedRefModelId) {
            const refModel = (models || []).find((m) => String(m.id) === String(selectedRefModelId));
            if (!refModel) {
                setError("Selected reference entity was not found.");
                return;
            }

            setForm((prev) => ({
                ...prev,
                columns_def: (refModel.columns_def || [DEFAULT_COL()]).map((c) => ({ ...DEFAULT_COL(), ...c })),
                object_type: refModel.object_type || "table",
                table_type: refModel.table_type || "regular",
                business_rules: refModel.business_rules || prev.business_rules,
            }));
            setError(null);
            return;
        }

        setError("Select a team template or reference entity first.");
    };

    // ── Fetch target schemas when target connection changes ──
    useEffect(() => {
        const connectionId = Number(form.target_connection_id);
        if (!Number.isInteger(connectionId) || connectionId <= 0) {
            setTargetSchemas([]);
            return;
        }
        let active = true;
        setTargetSchemasLoading(true);
        (async () => {
            try {
                const data = await fetchSchemas(connectionId, targetDatabaseName);
                if (!active) return;
                setTargetSchemas(data?.schemas || []);
            } catch {
                if (!active) return;
                setTargetSchemas([]);
            } finally {
                if (active) setTargetSchemasLoading(false);
            }
        })();
        return () => { active = false; };
    }, [form.target_connection_id, targetDatabaseName]);

    // ── Create schema (architect / admin only) ──
    const handleCreateSchema = async () => {
        if (!newSchemaName.trim()) return;
        if (!sessionPassword?.trim()) {
            setCreateSchemaError("Enter the database password in Explorer first, then retry schema creation.");
            return;
        }
        setCreateSchemaLoading(true);
        setCreateSchemaError("");
        try {
            const targetConnectionId = Number(form.target_connection_id);
            const createdSchema = newSchemaName.trim();
            await createSchema(targetConnectionId, createdSchema, targetDatabaseName, sessionPassword || "");
            if (
                String(explorerConnectionId || "") === String(targetConnectionId)
                && explorerDatabase
                && explorerDatabase === targetDatabaseName
            ) {
                setExplorerCache((prev) => {
                    const next = prev?.schemas || [];
                    if (next.includes(createdSchema)) return prev;
                    return {
                        ...prev,
                        schemas: [...next, createdSchema].sort((a, b) => a.localeCompare(b)),
                    };
                });
            }
            // Refresh schema list and auto-select the new schema
            const data = await fetchSchemas(targetConnectionId, targetDatabaseName);
            setTargetSchemas(data?.schemas || []);
            set("target_schema", createdSchema);
            setNewSchemaName("");
            setShowCreateSchema(false);
        } catch (err) {
            setCreateSchemaError(err.message || "Schema creation failed");
        } finally {
            setCreateSchemaLoading(false);
        }
    };

    // ── Save draft ──
    const saveDraft = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const payload = {
                name:                 form.name || "Unnamed Entity",
                description:          form.description,
                source_connection_id: form.source_connection_id || null,
                source_schema:        form.source_schema || null,
                source_tables:        form.source_tables,
                target_connection_id: form.target_connection_id || null,
                target_schema:        form.target_schema || null,
                object_type:          form.object_type,
                table_type:           form.object_type === "table" ? form.table_type : null,
                columns_def:          form.columns_def.filter((c) => c.name.trim()),
                transformations:      form.transformations || null,
                business_rules:       form.business_rules,
            };
            let result;
            if (savedId) {
                result = await updateModel(savedId, payload);
            } else {
                result = await createModel(payload);
                setSavedId(result.id);
            }
            onSaved(result);
            return result;
        } catch (err) {
            setError(err.message);
            return null;
        } finally {
            setLoading(false);
        }
    }, [form, savedId, onSaved]);

    // ── Move to next step ──
    const goNext = async () => {
        // Auto-save on every step transition
        const result = await saveDraft();
        if (!result) return;
        const idx = STEP_INDEX[step];
        if (idx < STEPS.length - 1) setStep(STEPS[idx + 1].id);
    };

    const goPrev = () => {
        const idx = STEP_INDEX[step];
        if (idx > 0) setStep(STEPS[idx - 1].id);
    };

    // ── Validate ──
    const runValidate = async () => {
        const saved = await saveDraft();
        if (!saved) return;
        setLoading(true);
        setError(null);
        try {
            const result = await validateModel(saved.id, form.business_rules);
            setValidated(result);
            onSaved(result);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    // ── Deploy ──
    const runDeploy = async () => {
        const saved = await saveDraft();
        const modelId = saved?.id || savedId;
        if (!modelId) {
            setError("Save the model before deployment.");
            return;
        }

        const targetConnectionId = Number(form.target_connection_id);
        if (!Number.isInteger(targetConnectionId) || targetConnectionId <= 0) {
            setError("Select a valid target connection before deployment.");
            return;
        }

        setLoading(true);
        setError(null);
        try {
            const result = await deployModel(
                modelId,
                targetConnectionId,
                form.target_schema,
            );
            setDeployed(result);
            onSaved(result);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const currentIdx = STEP_INDEX[step] ?? 0;
    const isLast = currentIdx === STEPS.length - 1;

    // ── Step content ──
    const renderStep = () => {
        switch (step) {
            // ── Step 1: Setup ──
            case "setup":
                return (
                    <>
                        <p className="mdp-section-sub">
                            Give your entity a name and optionally pick a column template.
                            Use the bulk section to create multiple entities at once.
                        </p>
                        <div className="mdp-field">
                            <label className="mdp-label">Entity Name *</label>
                            <input
                                className="mdp-input"
                                placeholder="e.g. fact_orders"
                                value={form.name}
                                onChange={(e) => set("name", e.target.value)}
                            />
                        </div>
                        <div className="mdp-field">
                            <label className="mdp-label">Description</label>
                            <input
                                className="mdp-input"
                                placeholder="Short description of this entity"
                                value={form.description}
                                onChange={(e) => set("description", e.target.value)}
                            />
                        </div>
                        <div className="mdp-divider" />
                        <div className="mdp-field">
                            <label className="mdp-label">Single Entity Template Strategy</label>
                            <select
                                className="mdp-select"
                                style={{ minWidth: 260 }}
                                value={templateMode}
                                onChange={(e) => {
                                    const next = e.target.value;
                                    setTemplateMode(next);
                                    setError(null);
                                    if (next === "custom") {
                                        set("source_schema", "");
                                        set("source_tables", []);
                                    } else {
                                        setSelectedTemplateId("");
                                        setSelectedRefModelId("");
                                    }
                                }}
                            >
                                <option value="custom">Custom Template / Reference Entity</option>
                                <option value="source">Source Object Template</option>
                            </select>
                            <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-muted)" }}>
                                Choose how to populate this current entity's columns and base type.
                            </div>

                            {templateMode === "custom" ? (
                                <>
                                    <div style={{ marginTop: 10 }}>
                                        <label className="mdp-label">Team Template</label>
                                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                                            <select
                                                className="mdp-select"
                                                style={{ minWidth: 260 }}
                                                value={selectedTemplateId}
                                                onChange={(e) => { setSelectedTemplateId(e.target.value); if (e.target.value) setSelectedRefModelId(""); }}
                                            >
                                                <option value="">No template (manual columns)</option>
                                                {templates.map((t) => (
                                                    <option key={t.id} value={t.id}>{t.name}</option>
                                                ))}
                                            </select>
                                            <button className="btn-secondary" onClick={() => setShowTemplateModal(true)}>
                                                Open Template Window
                                            </button>
                                            <button
                                                className="btn-secondary"
                                                onClick={applySingleTemplateSelection}
                                                disabled={!selectedTemplateId && !selectedRefModelId}
                                            >
                                                Apply to Current Entity
                                            </button>
                                        </div>
                                        <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-muted)" }}>
                                            Templates are stored in backend and shared across users.
                                        </div>
                                    </div>

                                    <div style={{ marginTop: 10 }}>
                                        <label className="mdp-label" style={{ marginBottom: 4 }}>— or — Use Existing Entity as Reference</label>
                                        <select
                                            className="mdp-select"
                                            style={{ minWidth: 260 }}
                                            value={selectedRefModelId}
                                            onChange={(e) => { setSelectedRefModelId(e.target.value); if (e.target.value) setSelectedTemplateId(""); }}
                                        >
                                            <option value="">No reference entity</option>
                                            {(models || []).filter((m) => m.id !== editingId).map((m) => (
                                                <option key={m.id} value={m.id}>{m.name} ({m.target_schema || "no schema"})</option>
                                            ))}
                                        </select>
                                        <div style={{ marginTop: 4, fontSize: 12, color: "var(--text-muted)" }}>
                                            Inherits column types, object type, table type, and business rules from the selected entity.
                                        </div>
                                    </div>
                                </>
                            ) : (
                                <div style={{ marginTop: 10, display: "grid", gap: 8 }}>
                                    <div style={{ fontSize: 12, color: "var(--text-muted)" }}>
                                        Using target connection for source lookup.
                                    </div>
                                    <div className="mdp-row">
                                        <div className="mdp-field">
                                            <label className="mdp-label">Source Schema</label>
                                            <select
                                                className="mdp-select"
                                                value={form.source_schema}
                                                onChange={(e) => {
                                                    set("source_schema", e.target.value);
                                                    set("source_tables", []);
                                                }}
                                                disabled={!sourceTemplateConnectionId || sourceTemplateSchemasLoading}
                                            >
                                                <option value="">— Select schema —</option>
                                                {sourceTemplateSchemas.map((s) => (
                                                    <option key={s} value={s}>{s}</option>
                                                ))}
                                            </select>
                                        </div>
                                    </div>

                                    <div className="mdp-row">
                                        <div className="mdp-field">
                                            <label className="mdp-label">Source Object</label>
                                            <select
                                                className="mdp-select"
                                                value={form.source_tables?.[0] || ""}
                                                onChange={(e) => set("source_tables", e.target.value ? [e.target.value] : [])}
                                                disabled={!form.source_schema || sourceTemplateObjectsLoading}
                                            >
                                                <option value="">— Select table/view —</option>
                                                {sourceTemplateObjects.map((obj) => (
                                                    <option key={`${obj.type || ""}:${obj.name}`} value={obj.name}>
                                                        {obj.name} ({obj.type || "OBJECT"})
                                                    </option>
                                                ))}
                                            </select>
                                        </div>
                                    </div>

                                    <div style={{ fontSize: 12, color: "var(--text-muted)" }}>
                                        Choose source schema and object to auto-fill columns and object type automatically.
                                    </div>
                                </div>
                            )}
                        </div>

                        <div className="mdp-divider" />
                        <div className="mdp-field">
                            <label className="mdp-label">Bulk Create Entities by Name</label>
                            <div style={{ marginBottom: 8, fontSize: 12, color: "var(--text-muted)" }}>
                                Paste entity names (one per line). Bulk template rules are configured in the section below.
                            </div>
                            <textarea
                                className="mdp-textarea"
                                style={{ minHeight: 100 }}
                                placeholder={"fact_orders\nfact_payments\ndim_customers\ndim_products"}
                                value={bulkNamesText}
                                onChange={(e) => setBulkNamesText(e.target.value)}
                            />
                            <div className="mdp-row" style={{ marginTop: 8 }}>
                                <div className="mdp-field">
                                    <label className="mdp-label">Target Connection</label>
                                    <select
                                        className="mdp-select"
                                        value={form.target_connection_id}
                                        onChange={(e) => {
                                            set("target_connection_id", e.target.value);
                                            set("target_schema", "");
                                            setTargetSchemas([]);
                                        }}
                                    >
                                        <option value="">— Select connection —</option>
                                        {connections.map((c) => (
                                            <option key={c.id} value={c.id}>{c.name} ({c.db_type})</option>
                                        ))}
                                    </select>
                                </div>
                                <div className="mdp-field">
                                    <label className="mdp-label">Target Schema</label>
                                    <select
                                        className="mdp-select"
                                        value={form.target_schema}
                                        onChange={(e) => set("target_schema", e.target.value)}
                                        disabled={!form.target_connection_id || targetSchemasLoading}
                                    >
                                        <option value="">— Select schema —</option>
                                        {targetSchemas.map((s) => (
                                            <option key={s} value={s}>{s}</option>
                                        ))}
                                    </select>
                                </div>
                            </div>
                            <button
                                className="btn-primary"
                                onClick={runBulkCreateByNames}
                                disabled={bulkBusy || !bulkNamesText.trim() || (bulkTemplateMode === "source" && !resolvedBulkSourceConnectionId)}
                            >
                                {bulkBusy ? <span className="mdp-spinner" /> : "Bulk Create Entities"}
                            </button>
                            {bulkResult && <div style={{ marginTop: 8, fontSize: 13, color: "var(--success-text)" }}>{bulkResult}</div>}
                        </div>

                        <div className="mdp-divider" />
                        <div className="mdp-field">
                            <label className="mdp-label">Bulk Template Strategy</label>
                            <select
                                className="mdp-select"
                                style={{ minWidth: 260 }}
                                value={bulkTemplateMode}
                                onChange={(e) => {
                                    const next = e.target.value;
                                    setBulkTemplateMode(next);
                                    setError(null);
                                    if (next !== "custom") {
                                        setBulkSelectedTemplateId("");
                                        setBulkSelectedRefModelId("");
                                    }
                                }}
                            >
                                <option value="custom">Custom Template / Reference Entity</option>
                                <option value="source">Source Object Template</option>
                            </select>
                            <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-muted)" }}>
                                This strategy is used only when you click Bulk Create Entities.
                                Use one line per target in the format: target_name, source_schema, source_object.
                            </div>

                            {bulkTemplateMode === "custom" ? (
                                <>
                                    <div style={{ marginTop: 10 }}>
                                        <label className="mdp-label">Bulk Team Template</label>
                                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                                            <select
                                                className="mdp-select"
                                                style={{ minWidth: 260 }}
                                                value={bulkSelectedTemplateId}
                                                onChange={(e) => { setBulkSelectedTemplateId(e.target.value); if (e.target.value) setBulkSelectedRefModelId(""); }}
                                            >
                                                <option value="">No template (manual columns)</option>
                                                {templates.map((t) => (
                                                    <option key={t.id} value={t.id}>{t.name}</option>
                                                ))}
                                            </select>
                                            <button className="btn-secondary" onClick={() => setShowTemplateModal(true)}>
                                                Open Template Window
                                            </button>
                                        </div>
                                    </div>

                                    <div style={{ marginTop: 10 }}>
                                        <label className="mdp-label" style={{ marginBottom: 4 }}>— or — Use Existing Entity as Bulk Reference</label>
                                        <select
                                            className="mdp-select"
                                            style={{ minWidth: 260 }}
                                            value={bulkSelectedRefModelId}
                                            onChange={(e) => { setBulkSelectedRefModelId(e.target.value); if (e.target.value) setBulkSelectedTemplateId(""); }}
                                        >
                                            <option value="">No reference entity</option>
                                            {(models || []).filter((m) => m.id !== editingId).map((m) => (
                                                <option key={m.id} value={m.id}>{m.name} ({m.target_schema || "no schema"})</option>
                                            ))}
                                        </select>
                                    </div>
                                </>
                            ) : (
                                <div style={{ marginTop: 10, display: "grid", gap: 8 }}>
                                    <div style={{ fontSize: 12, color: "var(--text-muted)" }}>
                                        Using target connection for source lookup.
                                        Enter one mapping per line, for example:
                                        <br />fact_orders, src, orders
                                        <br />fact_payments, src, payments
                                        <br />dim_customers, src, customers
                                    </div>
                                </div>
                            )}
                        </div>

                    </>
                );

            // ── Step 2: Entity Type ──
            case "type":
                return (
                    <>
                        <p className="mdp-section-sub">
                            Select whether you are building a <strong>Table</strong> or a <strong>View</strong>.
                            If a Table, choose the storage variant.
                        </p>
                        <div className="mdp-section-title">Object Type</div>
                        <div className="mdp-type-grid">
                            {[
                                { value: "table", icon: "🗂️", label: "Table",  desc: "A physical, persisted data object." },
                                { value: "view",  icon: "🔭", label: "View",   desc: "A saved SELECT query — no storage." },
                            ].map((opt) => (
                                <button
                                    key={opt.value}
                                    className={`mdp-type-btn ${form.object_type === opt.value ? "selected" : ""}`}
                                    onClick={() => set("object_type", opt.value)}
                                >
                                    <span className="mdp-type-btn-icon">{opt.icon}</span>
                                    <span className="mdp-type-btn-title">{opt.label}</span>
                                    <span className="mdp-type-btn-desc">{opt.desc}</span>
                                </button>
                            ))}
                        </div>

                        {form.object_type === "table" && (
                            <>
                                <div className="mdp-divider" style={{ margin: "16px 0" }} />
                                <div className="mdp-section-title">Table Variant</div>
                                <div className="mdp-type-grid" style={{ gridTemplateColumns: "1fr 1fr" }}>
                                    {TABLE_TYPES.map((opt) => (
                                        <button
                                            key={opt.value}
                                            className={`mdp-type-btn ${form.table_type === opt.value ? "selected" : ""}`}
                                            onClick={() => set("table_type", opt.value)}
                                        >
                                            <span className="mdp-type-btn-icon">{opt.icon}</span>
                                            <span className="mdp-type-btn-title">{opt.label}</span>
                                            <span className="mdp-type-btn-desc">{opt.desc}</span>
                                        </button>
                                    ))}
                                </div>
                            </>
                        )}
                    </>
                );

            // ── Step 3: Columns ──
            case "columns":
                return (
                    <>
                        <p className="mdp-section-sub">
                            Define the columns of your model. Use the drop-down to set data types.
                            Primary Key and Nullable flags will be reflected in the generated DDL.
                        </p>
                        <ColumnBuilder
                            columns={form.columns_def}
                            onChange={(cols) => set("columns_def", cols)}
                        />
                        {form.object_type === "view" && (
                            <div className="mdp-field" style={{ marginTop: 12 }}>
                                <label className="mdp-label">Advanced FROM / JOIN SQL (optional)</label>
                                <textarea
                                    className="mdp-textarea"
                                    style={{ minHeight: 120, fontFamily: '"Fira Code", "Cascadia Code", "Consolas", monospace', fontSize: 12 }}
                                    placeholder={`Example:\n(\n  SELECT\n    o.order_id,\n    o.order_number,\n    o.order_ts::date AS order_date\n  FROM src.orders o\n) q`}
                                    value={form.transformations?.view_from_clause || ""}
                                    onChange={(e) => set("transformations", { ...(form.transformations || {}), view_from_clause: e.target.value })}
                                />
                                <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-muted)" }}>
                                    Use this when columns come from joins or derived expressions. The SELECT list still follows the columns defined above.
                                </div>
                            </div>
                        )}
                        <div className="mdp-divider" style={{ margin: "12px 0" }} />
                        <div className="mdp-section-title">Generated DDL Preview</div>
                        <DDLPreview form={form} />
                    </>
                );

            // ── Step 4: Business Rules ──
            case "rules":
                return (
                    <>
                        <p className="mdp-section-sub">
                            Describe the business requirements for this model in plain language.
                            The AI will use this to score how well the model definition matches your intent.
                        </p>
                        <div className="mdp-field">
                            <label className="mdp-label">Business Requirements</label>
                            <textarea
                                className="mdp-textarea"
                                style={{ minHeight: 140 }}
                                placeholder={`e.g.\n• Must capture order_id, customer_id, order_date, total_amount\n• order_id is the primary key (BIGINT)\n• Grain: one row per order\n• Do NOT include PII columns`}
                                value={form.business_rules}
                                onChange={(e) => set("business_rules", e.target.value)}
                            />
                        </div>
                    </>
                );

            // ── Step 5: Validate ──
            case "validate":
                return (
                    <>
                        <p className="mdp-section-sub">
                            Run the AI validator. It will review your column definitions against the
                            stated business requirements and return an acceptance score (0 – 100 %).
                        </p>

                        {validated ? (
                            <div className="mdp-validation-card">
                                <div className="mdp-score-display">
                                    <div className={`mdp-score-circle ${scoreColor(validated.acceptance_score ?? 0)}`}>
                                        {validated.acceptance_score != null
                                            ? `${Math.round(validated.acceptance_score * 100)}%`
                                            : "—"}
                                    </div>
                                    <p className="mdp-score-notes">
                                        {validated.validation_notes || "No additional notes from AI."}
                                    </p>
                                </div>
                                <p className="mdp-validation-hint">
                                    {(validated.acceptance_score ?? 0) >= 0.8
                                        ? "✅ Model is well-aligned with requirements. You can proceed to Deploy."
                                        : (validated.acceptance_score ?? 0) >= 0.5
                                        ? "⚠️ Model partially matches requirements. Consider revising columns or business rules before deploying."
                                        : "❌ Low acceptance. Please review columns and business rules, then re-validate."}
                                </p>
                            </div>
                        ) : (
                            <div className="mdp-validation-card" style={{ alignItems: "center", padding: 32 }}>
                                <p style={{ color: "var(--text-muted)", textAlign: "center", marginBottom: 16 }}>
                                    Click <strong>Run AI Validation</strong> to score this model against your business requirements.
                                </p>
                                <button className="btn-primary" onClick={runValidate} disabled={loading}>
                                    {loading && <span className="mdp-spinner" />}
                                    Run AI Validation
                                </button>
                            </div>
                        )}

                        {validated && (
                            <button className="btn-secondary" style={{ alignSelf: "flex-start" }} onClick={runValidate} disabled={loading}>
                                {loading && <span className="mdp-spinner mdp-spinner-dark" />}
                                Re-validate
                            </button>
                        )}
                    </>
                );

            // ── Step 6: Deploy ──
            case "deploy":
                return (
                    <div className="mdp-deploy-wrap">
                        <p className="mdp-section-sub">
                            Choose the target connection and schema where the DDL should be physically executed.
                            The schema must already exist — architects can create a schema using the button below.
                        </p>
                        <div className="mdp-row">
                            <div className="mdp-field">
                                <label className="mdp-label">Target Connection</label>
                                <select
                                    className="mdp-select"
                                    value={form.target_connection_id}
                                    onChange={(e) => {
                                        set("target_connection_id", e.target.value);
                                        set("target_schema", "");
                                        setTargetSchemas([]);
                                        setShowCreateSchema(false);
                                    }}
                                >
                                    <option value="">— Select connection —</option>
                                    {connections.map((c) => (
                                        <option key={c.id} value={c.id}>{c.name} ({c.db_type})</option>
                                    ))}
                                </select>
                            </div>
                            <div className="mdp-field">
                                <label className="mdp-label">Target Schema</label>
                                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                                    <select
                                        className="mdp-select"
                                        value={form.target_schema}
                                        onChange={(e) => set("target_schema", e.target.value)}
                                        disabled={!form.target_connection_id || targetSchemasLoading}
                                    >
                                        <option value="">— Select schema —</option>
                                        {targetSchemas.map((s) => (
                                            <option key={s} value={s}>{s}</option>
                                        ))}
                                    </select>
                                    {canManageSchemas && form.target_connection_id && (
                                        <button
                                            className="btn-secondary"
                                            style={{ flexShrink: 0, fontSize: 12, whiteSpace: "nowrap" }}
                                            onClick={() => { setShowCreateSchema((v) => !v); setCreateSchemaError(""); }}
                                        >
                                            {showCreateSchema ? "Cancel" : "+ New Schema"}
                                        </button>
                                    )}
                                </div>
                                {targetSchemasLoading && (
                                    <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-muted)" }}>
                                        Loading schemas…
                                    </div>
                                )}
                                {!targetSchemasLoading && form.target_connection_id && targetSchemas.length === 0 && (
                                    <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-muted)" }}>
                                        No schemas found.{canManageSchemas ? " Use '+ New Schema' to create one." : " Ask an architect to create one."}
                                    </div>
                                )}
                            </div>
                        </div>

                        {showCreateSchema && canManageSchemas && (
                            <div style={{ background: "var(--bg-soft-2)", border: "1px solid var(--border)", borderRadius: 10, padding: 14, display: "flex", flexDirection: "column", gap: 8 }}>
                                <div style={{ fontSize: 13, fontWeight: 600 }}>Create New Schema</div>
                                <div style={{ display: "flex", gap: 8 }}>
                                    <input
                                        className="mdp-input"
                                        placeholder="schema_name"
                                        value={newSchemaName}
                                        onChange={(e) => setNewSchemaName(e.target.value)}
                                        onKeyDown={(e) => e.key === "Enter" && handleCreateSchema()}
                                        style={{ flex: 1 }}
                                    />
                                    <button
                                        className="btn-primary"
                                        style={{ flexShrink: 0 }}
                                        onClick={handleCreateSchema}
                                        disabled={createSchemaLoading || !newSchemaName.trim()}
                                    >
                                        {createSchemaLoading ? <span className="mdp-spinner" /> : "Create"}
                                    </button>
                                </div>
                                {createSchemaError && <div className="mdp-error">{createSchemaError}</div>}
                            </div>
                        )}

                        <div className="mdp-section-title">Final DDL</div>
                        <DDLPreview form={form} />

                        {deployed ? (
                            <div className="mdp-deployed-banner">
                                <CheckCircle size={18} />
                                Model deployed successfully as <strong>{deployed.name}</strong> in{" "}
                                <strong>{deployed.target_schema}</strong>.
                                You can now use it in the ETL / Lineage page.
                            </div>
                        ) : (
                            <button
                                className="btn-primary"
                                style={{ alignSelf: "flex-start", display: "flex", alignItems: "center", gap: 6 }}
                                onClick={runDeploy}
                                disabled={loading || !form.target_connection_id || !form.target_schema}
                            >
                                {loading ? <span className="mdp-spinner" /> : <Rocket size={14} />}
                                Deploy Model
                            </button>
                        )}
                    </div>
                );

            default:
                return null;
        }
    };

    return (
        <div className="mdp-wizard-overlay" onClick={(e) => e.target === e.currentTarget && onClose()}>
            <div className="mdp-wizard-panel">
                {/* Header */}
                <div className="mdp-wizard-header">
                    <span className="mdp-wizard-title">
                        {editingId ? "Edit Entity" : "New Entity"}
                        {form.name && ` — ${form.name}`}
                    </span>
                    <button className="btn-icon" onClick={onClose} title="Close"><X size={16} /></button>
                </div>

                <StepsBar currentStep={step} />

                {/* Body */}
                <div className="mdp-wizard-body">
                    {error && <div className="mdp-error">{error}</div>}
                    {renderStep()}
                </div>

                {/* Footer */}
                <div className="mdp-wizard-footer">
                    <div className="mdp-wizard-footer-left">
                        <button className="btn-secondary" onClick={goPrev} disabled={currentIdx === 0 || loading}>
                            ← Back
                        </button>
                    </div>
                    <div className="mdp-wizard-footer-right">
                        <button className="btn-secondary" onClick={saveDraft} disabled={loading}>
                            {loading ? <><span className="mdp-spinner mdp-spinner-dark" />Saving…</> : "Save Draft"}
                        </button>
                        {!isLast && (
                            <button
                                className="btn-primary"
                                onClick={step === "validate" ? () => setStep("deploy") : goNext}
                                disabled={loading || !form.name.trim()}
                            >
                                {loading ? <span className="mdp-spinner" /> : null}
                                {step === "validate" ? "Proceed to Deploy" : "Save & Continue"} →
                            </button>
                        )}
                    </div>
                </div>

                {showTemplateModal && (
                    <div className="mdp-wizard-overlay" style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.45)" }} onClick={(e) => e.target === e.currentTarget && setShowTemplateModal(false)}>
                        <div className="mdp-wizard-panel" style={{ maxWidth: 760, maxHeight: "86vh" }}>
                            <div className="mdp-wizard-header">
                                <span className="mdp-wizard-title">Create Team Template</span>
                                <button className="btn-icon" onClick={() => setShowTemplateModal(false)} title="Close"><X size={16} /></button>
                            </div>
                            <div className="mdp-wizard-body" style={{ display: "grid", gap: 12 }}>
                                <div className="mdp-field">
                                    <label className="mdp-label">Template Name *</label>
                                    <input className="mdp-input" value={templateName} onChange={(e) => setTemplateName(e.target.value)} placeholder="e.g. src_to_core_standard" />
                                </div>
                                <div className="mdp-field">
                                    <label className="mdp-label">Description</label>
                                    <input className="mdp-input" value={templateDescription} onChange={(e) => setTemplateDescription(e.target.value)} placeholder="Reusable column pattern for this entity type" />
                                </div>
                                <div style={{ fontSize: 12, color: "var(--text-muted)" }}>
                                    The current entity's columns will be saved as the template. Define columns first (Step 3), then open this window to save.
                                </div>
                                {templateError && <div className="mdp-error">{templateError}</div>}
                            </div>
                            <div className="mdp-wizard-footer">
                                <button className="btn-secondary" onClick={() => setShowTemplateModal(false)}>Cancel</button>
                                <button className="btn-primary" onClick={saveTeamTemplate} disabled={templateBusy}>
                                    {templateBusy ? <span className="mdp-spinner" /> : "Save Template"}
                                </button>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

// ── DDL live-preview (client-side, mirrors backend logic) ─────────────────────

function DDLPreview({ form }) {
    const schemaPrefix = form.target_schema ? `${form.target_schema}.` : "";
    const name = form.name || "unnamed_model";
    const cols = (form.columns_def || []).filter((c) => c.name.trim());
    let ddl = "";

    if (form.object_type === "view") {
        const selectCols = cols.length ? cols.map((c) => c.name).join(", ") : "*";
        const customFrom = String(form.transformations?.view_from_clause || "").trim();
        const quoteIdent = (v) => `"${String(v || "").replace(/"/g, '""')}"`;
        const sourceSchema = String(form.source_schema || "").trim();
        const formatSourceRef = (value) => {
            const ref = String(value || "").trim();
            if (!ref) return "";
            // Keep complex refs untouched (joins/aliases/subqueries).
            if (/[\s();]/.test(ref) || ref.includes(".")) return ref;
            return sourceSchema ? `${quoteIdent(sourceSchema)}.${quoteIdent(ref)}` : quoteIdent(ref);
        };
        const fromClause = customFrom
            ? customFrom.replace(/;+\s*$/, "")
            : form.source_tables?.length
                ? form.source_tables.map(formatSourceRef).filter(Boolean).join(", ")
                : "/* source_table */";
        ddl = `CREATE OR REPLACE VIEW ${schemaPrefix}${name} AS\nSELECT ${selectCols}\nFROM ${fromClause};`;
    } else {
        const prefixMap = {
            iceberg:   "CREATE ICEBERG TABLE",
            transient: "CREATE TRANSIENT TABLE",
            temporary: "CREATE TEMPORARY TABLE",
        };
        const ddlPrefix = prefixMap[form.table_type] || "CREATE TABLE IF NOT EXISTS";
        const pkCols = cols.filter((c) => c.primary_key).map((c) => c.name);
        const colLines = cols.map((c) => {
            let line = `    ${c.name} ${c.data_type}`;
            if (!c.nullable) line += " NOT NULL";
            if (c.unique) line += " UNIQUE";
            if (c.default_value) line += ` DEFAULT ${c.default_value}`;
            return line;
        });
        if (pkCols.length) colLines.push(`    PRIMARY KEY (${pkCols.join(", ")})`);
        const colsSql = colLines.length ? colLines.join(",\n") : "    id INTEGER";
        ddl = `${ddlPrefix} ${schemaPrefix}${name} (\n${colsSql}\n);`;
    }

    return (
        <div className="mdp-ddl-wrap">
            <pre className="mdp-ddl-code">{ddl}</pre>
        </div>
    );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

/** Group models as: database_name → schema → [models]. Undeployed = draft bucket. */
function groupModels(models, connections) {
    const byDb = {};
    const singleDbFallback = connections.length === 1 ? (connections[0]?.database_name || "") : "";

    const findConn = (id) => connections.find((c) => String(c.id) === String(id));

    for (const m of models) {
        const targetConn = findConn(m.target_connection_id);
        const sourceConn = findConn(m.source_connection_id);

        let dbName = targetConn?.database_name || sourceConn?.database_name || "";
        if (!dbName && m.target_connection_id && singleDbFallback) {
            dbName = singleDbFallback;
        }

        const dbKey = m.target_connection_id
            ? (dbName || `Connection #${m.target_connection_id}`)
            : "__draft__";

        if (!byDb[dbKey]) byDb[dbKey] = {};
        const schema = m.target_schema || "__no_schema__";
        if (!byDb[dbKey][schema]) byDb[dbKey][schema] = [];
        byDb[dbKey][schema].push(m);
    }
    return byDb;
}

export default function ModelDevelopmentPage() {
    const [models, setModels]           = useState([]);
    const [connections, setConnections] = useState([]);
    const [loading, setLoading]         = useState(true);
    const [error, setError]             = useState(null);
    const [wizardOpen, setWizardOpen]   = useState(false);
    const [editingModel, setEditingModel] = useState(null);  // null = new, object = edit
    const [collapsedConns, setCollapsedConns]     = useState({});
    const [collapsedSchemas, setCollapsedSchemas] = useState({});
    const [selectedModelIds, setSelectedModelIds] = useState(new Set());
    const [bulkDeleting, setBulkDeleting] = useState(false);
    const [bulkDeploying, setBulkDeploying] = useState(false);

    // ── Create Schema panel (for admin / architect) ──
    const [showSchemaMgmt, setShowSchemaMgmt]     = useState(false);
    const [schemaMgmtConn, setSchemaMgmtConn]     = useState("");
    const [schemaMgmtName, setSchemaMgmtName]     = useState("");
    const [schemaMgmtLoading, setSchemaMgmtLoading] = useState(false);
    const [schemaMgmtError, setSchemaMgmtError]   = useState("");
    const [schemaMgmtSuccess, setSchemaMgmtSuccess] = useState("");

    const { user } = useAuth();
    const {
        connectionId: explorerConnectionId,
        selectedDatabase: explorerDatabase,
        setExplorerCache,
        sessionPassword,
    } = useAppContext();
    const canManageSchemas = user?.role === "admin" || user?.role === "architect";

    const loadData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const [m, c] = await Promise.all([listModels(), fetchSavedConnections()]);
            setModels(m || []);
            setConnections(dedupeConnections(c || []));
        } catch (err) {
            const msg = err?.message || "Request failed";
            if (msg.toLowerCase().includes("failed to fetch")) {
                setError("Unable to reach backend. Check API server and login session.");
            } else {
                setError(msg);
            }
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => { loadData(); }, [loadData]);

    const openNew = () => {
        setEditingModel(null);
        setWizardOpen(true);
    };

    const openEdit = (model) => {
        setEditingModel(model);
        setWizardOpen(true);
    };

    const handleWizardSaved = (model) => {
        setModels((prev) => {
            const idx = prev.findIndex((m) => m.id === model.id);
            if (idx >= 0) {
                const next = [...prev];
                next[idx] = model;
                return next;
            }
            return [model, ...prev];
        });
    };

    const handleDelete = async (e, modelId) => {
        e.stopPropagation();
        if (!confirm("Delete this entity?")) return;
        try {
            await deleteModel(modelId);
            setModels((prev) => prev.filter((m) => m.id !== modelId));
            setSelectedModelIds((prev) => { const next = new Set(prev); next.delete(modelId); return next; });
        } catch (err) {
            alert(err.message);
        }
    };

    const handleBulkDelete = async () => {
        if (selectedModelIds.size === 0) return;
        if (!confirm(`Delete ${selectedModelIds.size} selected entity(ies)? This cannot be undone.`)) return;
        setBulkDeleting(true);
        const ids = [...selectedModelIds];
        const failed = [];
        for (const id of ids) {
            try {
                await deleteModel(id);
            } catch {
                failed.push(id);
            }
        }
        setModels((prev) => prev.filter((m) => !ids.includes(m.id) || failed.includes(m.id)));
        setSelectedModelIds(new Set(failed));
        setBulkDeleting(false);
        if (failed.length) alert(`${failed.length} entity(ies) could not be deleted.`);
    };

    const handleBulkDeploy = async () => {
        if (selectedModelIds.size === 0) return;
        const ids = [...selectedModelIds];
        const selectedModels = models.filter((m) => ids.includes(m.id));
        const missingTargets = selectedModels.filter((m) => !m.target_connection_id || !m.target_schema);
        if (missingTargets.length) {
            alert(`${missingTargets.length} selected entity(ies) are missing target connection/schema. Set target details first, then retry.`);
            return;
        }
        if (!confirm(`Deploy ${selectedModelIds.size} selected entity(ies) now?`)) return;

        setBulkDeploying(true);
        const failed = [];
        for (const model of selectedModels) {
            try {
                await deployModel(
                    model.id,
                    Number(model.target_connection_id),
                    String(model.target_schema || "").trim(),
                );
            } catch {
                failed.push(model.id);
            }
        }

        await loadData();
        setSelectedModelIds(new Set(failed));
        setBulkDeploying(false);
        if (failed.length) alert(`${failed.length} entity(ies) could not be deployed.`);
    };

    const toggleModelSelection = (e, modelId) => {
        e.stopPropagation();
        setSelectedModelIds((prev) => {
            const next = new Set(prev);
            next.has(modelId) ? next.delete(modelId) : next.add(modelId);
            return next;
        });
    };

    const selectAllInSchema = (e, sModels) => {
        e.stopPropagation();
        const ids = sModels.map((m) => m.id);
        const allSelected = ids.every((id) => selectedModelIds.has(id));
        setSelectedModelIds((prev) => {
            const next = new Set(prev);
            if (allSelected) { ids.forEach((id) => next.delete(id)); }
            else { ids.forEach((id) => next.add(id)); }
            return next;
        });
    };

    const toggleConn = (connId) => setCollapsedConns((p) => ({ ...p, [connId]: !p[connId] }));
    const toggleSchema = (key) => setCollapsedSchemas((p) => ({ ...p, [key]: !p[key] }));

    const handleCreateSchemaFromPanel = async () => {
        if (!schemaMgmtConn || !schemaMgmtName.trim()) return;
        if (!sessionPassword?.trim()) {
            setSchemaMgmtError("Enter the database password in Explorer first, then retry schema creation.");
            return;
        }
        setSchemaMgmtLoading(true);
        setSchemaMgmtError("");
        setSchemaMgmtSuccess("");
        try {
            const selectedConn = connections.find((c) => String(c.id) === String(schemaMgmtConn));
            const dbName = selectedConn?.database_name || "";
            const createdSchema = schemaMgmtName.trim();
            await createSchema(Number(schemaMgmtConn), createdSchema, dbName, sessionPassword || "");
            if (
                String(explorerConnectionId || "") === String(schemaMgmtConn)
                && explorerDatabase
                && explorerDatabase === dbName
            ) {
                setExplorerCache((prev) => {
                    const next = prev?.schemas || [];
                    if (next.includes(createdSchema)) return prev;
                    return {
                        ...prev,
                        schemas: [...next, createdSchema].sort((a, b) => a.localeCompare(b)),
                    };
                });
            }
            setSchemaMgmtSuccess(`Schema "${createdSchema}" created successfully.`);
            setSchemaMgmtName("");
        } catch (err) {
            setSchemaMgmtError(err.message || "Schema creation failed");
        } finally {
            setSchemaMgmtLoading(false);
        }
    };

    const grouped = groupModels(models, connections);
    const hasExplorerConnection = connections.some((c) => String(c.id) === String(explorerConnectionId || ""));
    const validExplorerConnectionId = hasExplorerConnection ? explorerConnectionId : "";

    const dbLabel = (dbKey) => {
        if (dbKey === "__draft__") return "Drafts / Undeployed";
        return dbKey;
    };

    return (
        <div className="mdp-page">
            {/* Toolbar */}
            <div className="mdp-toolbar">
                <span className="mdp-toolbar-title">
                    <BoxSelect size={16} /> Saved Entities ({models.length})
                </span>
                <div className="mdp-toolbar-actions">
                    {selectedModelIds.size > 0 && (
                        <button
                            className="btn-primary"
                            style={{ display: "flex", alignItems: "center", gap: 6 }}
                            onClick={handleBulkDeploy}
                            disabled={bulkDeploying || bulkDeleting}
                        >
                            {bulkDeploying ? <span className="mdp-spinner" /> : <Rocket size={14} />}
                            Deploy Selected ({selectedModelIds.size})
                        </button>
                    )}
                    {selectedModelIds.size > 0 && (
                        <button
                            className="btn-danger"
                            style={{ display: "flex", alignItems: "center", gap: 6 }}
                            onClick={handleBulkDelete}
                            disabled={bulkDeleting || bulkDeploying}
                        >
                            {bulkDeleting ? <span className="mdp-spinner" /> : <Trash2 size={14} />}
                            Delete Selected ({selectedModelIds.size})
                        </button>
                    )}
                    {selectedModelIds.size > 0 && (
                        <button className="btn-secondary" onClick={() => setSelectedModelIds(new Set())}>
                            Clear Selection
                        </button>
                    )}
                    {canManageSchemas && (
                        <button
                            className="btn-secondary"
                            style={{ display: "flex", alignItems: "center", gap: 6 }}
                            onClick={() => { setShowSchemaMgmt((v) => !v); setSchemaMgmtError(""); setSchemaMgmtSuccess(""); }}
                        >
                            <Layers size={14} /> {showSchemaMgmt ? "Close Schema Manager" : "Create Schema"}
                        </button>
                    )}
                    <button className="btn-primary" style={{ display: "flex", alignItems: "center", gap: 6 }} onClick={openNew}>
                        <Plus size={14} /> New Entity
                    </button>
                </div>
            </div>

            {/* Schema Management Panel (admin / architect only) */}
            {canManageSchemas && showSchemaMgmt && (
                <div style={{ margin: "0 0 16px 0", background: "var(--bg-soft)", border: "1px solid var(--border)", borderRadius: 12, padding: "16px 20px" }}>
                    <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 12, display: "flex", alignItems: "center", gap: 8 }}>
                        <Layers size={15} style={{ color: "var(--accent)" }} /> Schema Management
                    </div>
                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "flex-end" }}>
                        <div style={{ display: "flex", flexDirection: "column", gap: 4, flex: "1 1 200px" }}>
                            <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Target Connection</label>
                            <select
                                className="mdp-select"
                                value={schemaMgmtConn}
                                onChange={(e) => { setSchemaMgmtConn(e.target.value); setSchemaMgmtError(""); setSchemaMgmtSuccess(""); }}
                            >
                                <option value="">— Select connection —</option>
                                {connections.map((c) => (
                                    <option key={c.id} value={c.id}>{c.name} ({c.db_type})</option>
                                ))}
                            </select>
                        </div>
                        <div style={{ display: "flex", flexDirection: "column", gap: 4, flex: "1 1 200px" }}>
                            <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Schema Name</label>
                            <input
                                className="mdp-input"
                                placeholder="e.g. analytics, raw, staging"
                                value={schemaMgmtName}
                                onChange={(e) => { setSchemaMgmtName(e.target.value); setSchemaMgmtError(""); setSchemaMgmtSuccess(""); }}
                                onKeyDown={(e) => e.key === "Enter" && handleCreateSchemaFromPanel()}
                            />
                        </div>
                        <button
                            className="btn-primary"
                            style={{ height: 38, flexShrink: 0 }}
                            onClick={handleCreateSchemaFromPanel}
                            disabled={schemaMgmtLoading || !schemaMgmtConn || !schemaMgmtName.trim()}
                        >
                            {schemaMgmtLoading ? <span className="mdp-spinner" /> : "Create Schema"}
                        </button>
                    </div>
                    {schemaMgmtError && <div className="mdp-error" style={{ marginTop: 10 }}>{schemaMgmtError}</div>}
                    {schemaMgmtSuccess && (
                        <div style={{ marginTop: 10, padding: "8px 12px", background: "var(--success-bg)", border: "1px solid var(--success-border)", borderRadius: 8, color: "var(--success-text)", fontSize: 13 }}>
                            {schemaMgmtSuccess}
                        </div>
                    )}
                </div>
            )}

            {/* Hierarchical model list */}
            <div className="mdp-grid-wrap">
                {loading && (
                    <div className="mdp-empty">
                        <span className="mdp-spinner mdp-spinner-dark" style={{ width: 28, height: 28, borderWidth: 3 }} />
                        <span style={{ color: "var(--text-muted)" }}>Loading…</span>
                    </div>
                )}

                {!loading && error && <div className="mdp-error">{error}</div>}

                {!loading && !error && models.length === 0 && (
                    <div className="mdp-empty">
                        <span className="mdp-empty-icon">🧱</span>
                        <div className="mdp-empty-title">No entities yet</div>
                        <div className="mdp-empty-sub">
                            Create your first entity. Define columns, choose a type
                            (Table, View, Iceberg…), validate with AI, then deploy to your target.
                        </div>
                        <button className="btn-primary" onClick={openNew}>
                            <Plus size={14} style={{ verticalAlign: "middle", marginRight: 4 }} />
                            Create First Entity
                        </button>
                    </div>
                )}

                {!loading && !error && models.length > 0 && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                        {Object.entries(grouped).map(([dbKey, schemaMap]) => {
                            const connCollapsed = collapsedConns[dbKey];
                            return (
                                <div key={dbKey} style={{ border: "1px solid var(--border)", borderRadius: 12, overflow: "hidden" }}>
                                    {/* Database header */}
                                    <div
                                        style={{ display: "flex", alignItems: "center", gap: 8, padding: "10px 16px", background: "var(--bg-soft)", cursor: "pointer", userSelect: "none", fontWeight: 700, fontSize: 14 }}
                                        onClick={() => toggleConn(dbKey)}
                                    >
                                        {connCollapsed ? <ChevronRightIcon size={16} /> : <ChevronDown size={16} />}
                                        <Database size={15} style={{ color: "var(--accent)" }} />
                                        <span>{dbLabel(dbKey)}</span>
                                    </div>

                                    {!connCollapsed && (
                                        <div style={{ padding: "8px 0" }}>
                                            {Object.entries(schemaMap).map(([schema, sModels]) => {
                                                const schemaKey = `${dbKey}::${schema}`;
                                                const schemaCollapsed = collapsedSchemas[schemaKey];
                                                const schemaLabel = schema === "__no_schema__" ? "No Schema" : schema;
                                                const allSchemaSelected = sModels.every((m) => selectedModelIds.has(m.id));
                                                return (
                                                    <div key={schema} style={{ margin: "0 8px 8px 8px", border: "1px solid var(--border)", borderRadius: 10, overflow: "hidden" }}>
                                                        {/* Schema header */}
                                                        <div
                                                            style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 12px", background: "var(--bg-soft-2)", cursor: "pointer", userSelect: "none", fontSize: 13 }}
                                                            onClick={() => toggleSchema(schemaKey)}
                                                        >
                                                            {schemaCollapsed ? <ChevronRightIcon size={14} /> : <ChevronDown size={14} />}
                                                            <Layers size={13} style={{ color: "var(--text-muted)" }} />
                                                            <span style={{ fontWeight: 600 }}>{schemaLabel}</span>
                                                            <span style={{ marginLeft: "auto", fontSize: 11, color: "var(--text-muted)" }}>{sModels.length} object{sModels.length !== 1 ? "s" : ""}</span>
                                                            <button
                                                                className="btn-secondary"
                                                                style={{ fontSize: 11, padding: "2px 8px", marginLeft: 8 }}
                                                                onClick={(e) => selectAllInSchema(e, sModels)}
                                                                title="Select/deselect all in this schema"
                                                            >
                                                                {allSchemaSelected ? "Deselect All" : "Select All"}
                                                            </button>
                                                        </div>

                                                        {!schemaCollapsed && (
                                                            <div className="mdp-grid" style={{ padding: "10px 10px 4px 10px" }}>
                                                                {sModels.map((m) => {
                                                                    const score = m.acceptance_score;
                                                                    const pct = score != null ? Math.round(score * 100) : null;
                                                                    const barColor = score != null
                                                                        ? score >= 0.8 ? "var(--success-text)" : score >= 0.5 ? "#f59e0b" : "var(--danger-text)"
                                                                        : "var(--border)";
                                                                    const isSelected = selectedModelIds.has(m.id);
                                                                    return (
                                                                        <div key={m.id} className={`mdp-card${isSelected ? " mdp-card-selected" : ""}`} onClick={() => openEdit(m)}>
                                                                            <div className="mdp-card-header">
                                                                                <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
                                                                                    <input
                                                                                        type="checkbox"
                                                                                        checked={isSelected}
                                                                                        onChange={(e) => toggleModelSelection(e, m.id)}
                                                                                        onClick={(e) => e.stopPropagation()}
                                                                                        style={{ cursor: "pointer", width: 14, height: 14, flexShrink: 0 }}
                                                                                        title="Select for bulk action"
                                                                                    />
                                                                                    <div className="mdp-card-name" style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.name}</div>
                                                                                </div>
                                                                                <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
                                                                                    <button className="btn-icon" onClick={(e) => { e.stopPropagation(); openEdit(m); }} title="Edit">
                                                                                        <Pencil size={13} />
                                                                                    </button>
                                                                                    <button className="btn-icon" style={{ color: "var(--danger-text)" }} onClick={(e) => handleDelete(e, m.id)} title="Delete">
                                                                                        <Trash2 size={13} />
                                                                                    </button>
                                                                                </div>
                                                                            </div>

                                                                            {m.description && <div className="mdp-card-desc">{m.description}</div>}

                                                                            <div className="mdp-card-meta">
                                                                                {statusBadge(m.status)}
                                                                                <span className="mdp-badge mdp-badge-type">
                                                                                    {m.object_type === "view" ? "VIEW" : (m.table_type || "TABLE").toUpperCase()}
                                                                                </span>
                                                                                <span style={{ fontSize: 11, color: "var(--text-muted)" }}>v{m.version}</span>
                                                                            </div>

                                                                            {pct != null && (
                                                                                <div className="mdp-card-score">
                                                                                    <span>AI Score:</span>
                                                                                    <div className="mdp-score-bar-wrap">
                                                                                        <div className="mdp-score-bar" style={{ width: `${pct}%`, background: barColor }} />
                                                                                    </div>
                                                                                    <span style={{ fontWeight: 700, color: barColor }}>{pct}%</span>
                                                                                </div>
                                                                            )}

                                                                            <div className="mdp-info-row">
                                                                                {m.source_schema && <span className="mdp-info-pill">src: {m.source_schema}</span>}
                                                                                {(m.columns_def?.length ?? 0) > 0 && (
                                                                                    <span className="mdp-info-pill">{m.columns_def.length} cols</span>
                                                                                )}
                                                                            </div>
                                                                        </div>
                                                                    );
                                                                })}
                                                            </div>
                                                        )}
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    )}
                                </div>
                            );
                        })}
                    </div>
                )}
            </div>

            {/* Wizard */}
            {wizardOpen && (
                <WizardPanel
                    draft={editingModel ? {
                        ...editingModel,
                        source_connection_id: editingModel.source_connection_id ?? "",
                        target_connection_id: editingModel.target_connection_id ?? "",
                    } : {
                        ...blankDraft(),
                        target_connection_id: validExplorerConnectionId || "",
                    }}
                    connections={connections}
                    models={models}
                    editingId={editingModel?.id || null}
                    onClose={() => setWizardOpen(false)}
                    onSaved={handleWizardSaved}
                    onBulkCreated={loadData}
                />
            )}
        </div>
    );
}
