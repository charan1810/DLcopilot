import { useEffect, useMemo, useState, useCallback, useRef } from "react";
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
    aiSuggestPipelineMapping,
    fetchSchemas,
    fetchObjects,
    fetchObjectDetails,
    fetchRelationships,
} from "../api/schemaApi";
import PipelineRunsTab from "./PipelineRunsTab";
import PipelineScheduleTab from "./PipelineScheduleTab";

const STARTER_TEMPLATES = {
    core: {
        id: "core",
        label: "Core / Current State",
        description:
            "Build a downstream schema using metadata-aware update and insert logic so existing target rows are refreshed without truncating the table.",
        targetSchemaSuffix: "_core",
        tableSuffix: "_core",
    },
    full_refresh: {
        id: "full_refresh",
        label: "Full Refresh Copy",
        description:
            "Rebuild the target table from scratch on every run by truncating the target before reload. Use only when full replacement is explicitly required.",
        targetSchemaSuffix: "_refresh",
        tableSuffix: "_fr",
    },
    history: {
        id: "history",
        label: "Historical Snapshot",
        description:
            "Append periodic snapshots into a history schema so each load preserves a time-stamped copy.",
        targetSchemaSuffix: "_history",
        tableSuffix: "_hist",
    },
    scd2: {
        id: "scd2",
        label: "SCD Type 2 Starter",
        description:
            "Create reviewed SCD2 table scaffolds for every table, including validity columns and change tracking placeholders.",
        targetSchemaSuffix: "_dim",
        tableSuffix: "_scd2",
    },
};

function getStarterTemplate(templateId) {
    return STARTER_TEMPLATES[templateId] || STARTER_TEMPLATES.core;
}

function getTemplateDefaultTargetSchema(schemaName, template) {
    if (!template) return schemaName || "";
    if (template.id === "core") return "CORE";
    return schemaName ? `${schemaName}${template.targetSchemaSuffix}` : "";
}

function buildStarterPipelineName(schemaName, templateLabel) {
    if (!schemaName) return "";
    const compactLabel = templateLabel.replace(/\s*\/\s*/g, " ").replace(/\s+Layer$/i, "").trim();
    return `${schemaName} ${compactLabel} Pipeline`;
}

function getDefaultStarterScope(selectedObject) {
    return selectedObject ? "current-object" : "all-tables";
}

function buildStarterDescription(templateId, schemaName, objectCount, targetSchema, selectedObject, scope) {
    if (templateId === "full_refresh") {
        return scope === "current-object"
            ? `Fully rebuild ${selectedObject} into ${targetSchema} on each run by truncating only the downstream target before reload.`
            : `Fully rebuild ${objectCount} tables from ${schemaName} into ${targetSchema} on each run by truncating only the downstream targets before reload.`;
    }

    if (templateId === "history") {
        return scope === "current-object"
            ? `Snapshot ${selectedObject} into ${targetSchema} with a load timestamp on every run.`
            : `Snapshot ${objectCount} tables from ${schemaName} into ${targetSchema} so every run keeps a historical copy.`;
    }

    if (templateId === "scd2") {
        return scope === "current-object"
            ? `Create an SCD Type 2 starter scaffold for ${selectedObject} in ${targetSchema}. Review natural keys before running change logic.`
            : `Create SCD Type 2 starter scaffolds for ${objectCount} tables from ${schemaName} in ${targetSchema}. Review natural keys before running change logic.`;
    }

    return scope === "current-object"
        ? `Synchronize ${selectedObject} into ${targetSchema} using non-destructive current-state update and insert logic.`
        : `Synchronize ${objectCount} tables from ${schemaName} into ${targetSchema} using non-destructive current-state update and insert logic.`;
}

function buildDefaultStarterConfig(schemaName, selectedObject) {
    const template = getStarterTemplate("core");
    const defaultTargetSchema = getTemplateDefaultTargetSchema(schemaName, template);
    const defaultScope = getDefaultStarterScope(selectedObject);
    return {
        scope: defaultScope,
        templateId: template.id,
        targetSchema: defaultTargetSchema,
        tableSuffix: template.tableSuffix,
        pipelineName: buildStarterPipelineName(schemaName, template.label),
        pipelineDescription: buildStarterDescription(
            template.id,
            schemaName,
            defaultScope === "current-object" ? 1 : 0,
            defaultTargetSchema,
            selectedObject,
            defaultScope
        ),
        includeViews: false,
    };
}

function buildDefaultPipelineForm(connectionId, databaseName, schemaName, selectedObject) {
    return {
        name: "",
        description: "",
        connection_id: connectionId || "",
        database_name: databaseName || "",
        schema_name: schemaName || "",
        source_object: selectedObject || "",
        target_object: "",
        mapping_config: buildDefaultPipelineMapping(schemaName, selectedObject, ""),
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
        starterConfig: {
            ...buildDefaultStarterConfig(schemaName, selectedObject),
            ...(session?.starterConfig || {}),
        },
    };
}

function quoteIdentifier(identifier) {
    return `"${String(identifier || "").replace(/"/g, '""')}"`;
}

function qualifyObject(schemaName, objectName) {
    return `${quoteIdentifier(schemaName)}.${quoteIdentifier(objectName)}`;
}

function appendObjectSuffix(objectName, suffix) {
    if (!suffix) return objectName;
    return objectName.endsWith(suffix) ? objectName : `${objectName}${suffix}`;
}

function formatColumnReference(columnName, alias = "") {
    return alias ? `${alias}.${quoteIdentifier(columnName)}` : quoteIdentifier(columnName);
}

function buildColumnList(columns = []) {
    return columns.map((column) => quoteIdentifier(column.column_name)).join(", ");
}

function buildColumnProjection(columns = [], alias = "src") {
    return columns.map((column) => formatColumnReference(column.column_name, alias)).join(",\n    ");
}

function buildHashExpression(columns = [], alias = "src") {
    const expressionParts = columns.map((column) => {
        const reference = formatColumnReference(column.column_name, alias);
        return `COALESCE(${reference}::text, '')`;
    });

    if (!expressionParts.length) {
        return `md5('static-row')`;
    }

    return `md5(concat_ws('||', ${expressionParts.join(", ")}))`;
}

function buildChangeDetectionCondition(columns = [], leftAlias = "tgt", rightAlias = "src") {
    const comparisons = columns.map((column) => (
        `${formatColumnReference(column.column_name, leftAlias)} IS DISTINCT FROM ${formatColumnReference(column.column_name, rightAlias)}`
    ));

    return comparisons.length ? comparisons.join("\n        OR ") : "FALSE";
}

function buildUpdateAssignments(columns = [], sourceAlias = "src") {
    return columns
        .map((column) => `${quoteIdentifier(column.column_name)} = ${formatColumnReference(column.column_name, sourceAlias)}`)
        .join(",\n       ");
}

function buildFullRowMatchCondition(columns = [], leftAlias = "tgt", rightAlias = "src") {
    const comparisons = columns.map((column) => (
        `${formatColumnReference(column.column_name, leftAlias)} IS NOT DISTINCT FROM ${formatColumnReference(column.column_name, rightAlias)}`
    ));

    return comparisons.length ? comparisons.join("\n      AND ") : "TRUE";
}

function buildJoinCondition(keyColumns = [], leftAlias = "tgt", rightAlias = "src") {
    if (!keyColumns.length) {
        return "TRUE";
    }

    return keyColumns
        .map((columnName) => `${formatColumnReference(columnName, leftAlias)} = ${formatColumnReference(columnName, rightAlias)}`)
        .join("\n    AND ");
}

function singularizeName(name = "") {
    const normalized = String(name || "").toLowerCase();
    if (normalized.endsWith("ies")) return `${normalized.slice(0, -3)}y`;
    if (normalized.endsWith("ses")) return normalized.slice(0, -2);
    if (normalized.endsWith("s") && !normalized.endsWith("ss")) return normalized.slice(0, -1);
    return normalized;
}

function inferBusinessKeys(objectName, columns = [], primaryKeys = []) {
    if (primaryKeys.length) {
        return {
            keys: primaryKeys,
            source: "primary_keys",
        };
    }

    const sourceStem = singularizeName(objectName);
    const auditLikeNames = new Set([
        "created_at",
        "updated_at",
        "deleted_at",
        "valid_from",
        "valid_to",
        "is_current",
        "snapshot_at",
        "snapshot_batch_id",
        "load_timestamp",
        "ingested_at",
    ]);

    const scored = columns.map((column, index) => {
        const name = String(column.column_name || "");
        const lower = name.toLowerCase();
        let score = 0;

        if (auditLikeNames.has(lower)) {
            score = -100;
        } else if (lower === "id") {
            score = 100;
        } else if (sourceStem && lower === `${sourceStem}_id`) {
            score = 96;
        } else if (/_id$/.test(lower)) {
            score = 90;
        } else if (/_key$/.test(lower)) {
            score = 84;
        } else if (/(^|_)(code|number|num)$/.test(lower)) {
            score = 74;
        } else if (/email$/.test(lower)) {
            score = 68;
        } else if (String(column.is_nullable || "").toUpperCase() === "NO") {
            score = 48;
        } else if (index === 0) {
            score = 20;
        }

        return { name, score, index };
    });

    const idLike = scored
        .filter((item) => item.score >= 90)
        .sort((a, b) => b.score - a.score || a.index - b.index)
        .map((item) => item.name);

    if (idLike.length >= 1 && idLike.length <= 2) {
        return {
            keys: idLike,
            source: "inferred_ids",
        };
    }

    const best = scored
        .filter((item) => item.score > 0)
        .sort((a, b) => b.score - a.score || a.index - b.index)
        .slice(0, 1)
        .map((item) => item.name);

    return {
        keys: best,
        source: best.length ? "inferred_best_match" : "none",
    };
}

function normalizeStarterObjectMetadata(item, details, relationships) {
    const columns = Array.isArray(details?.columns) ? details.columns : [];
    const explicitPrimaryKeys = Array.isArray(relationships?.primary_keys)
        ? relationships.primary_keys.filter(Boolean)
        : [];
    const keySelection = inferBusinessKeys(item.name, columns, explicitPrimaryKeys);
    const keySet = new Set(keySelection.keys);
    const trackedColumns = columns.filter((column) => !keySet.has(column.column_name));

    return {
        name: item.name,
        type: item.type || details?.object_type || "",
        columns,
        primaryKeys: explicitPrimaryKeys,
        businessKeys: keySelection.keys,
        keySource: keySelection.source,
        trackedColumns: trackedColumns.length ? trackedColumns : columns,
    };
}

function normalizeAvailableObjects(availableObjects = []) {
    return availableObjects
        .map((item) => {
            if (typeof item === "string") {
                return { name: item, type: "" };
            }

            if (!item?.name) {
                return null;
            }

            return {
                name: item.name,
                type: item.type || "",
            };
        })
        .filter(Boolean);
}

function selectStarterObjects(availableObjects, selectedObject, starterConfig) {
    const normalizedObjects = normalizeAvailableObjects(availableObjects);

    if (starterConfig.scope === "current-object") {
        const currentObject = normalizedObjects.find((item) => item.name === selectedObject);
        return currentObject
            ? [currentObject]
            : selectedObject
                ? [{ name: selectedObject, type: "" }]
                : [];
    }

    const includeViews = !!starterConfig.includeViews;
    const filtered = normalizedObjects.filter((item) => {
        const objectType = String(item.type || "").toLowerCase();

        if (!objectType) {
            return true;
        }

        if (objectType.includes("table")) {
            return true;
        }

        return includeViews && objectType.includes("view");
    });

    if (filtered.length > 0) {
        return filtered;
    }

    return normalizedObjects.length > 0
        ? normalizedObjects
        : selectedObject
            ? [{ name: selectedObject, type: "" }]
            : [];
}

function buildCoreStepSql(sourceSchema, targetSchema, objectMeta, targetObject) {
    const sourceObject = objectMeta.name;
    const sourceRef = qualifyObject(sourceSchema, sourceObject);
    const targetRef = qualifyObject(targetSchema, targetObject);
    const columns = Array.isArray(objectMeta.columns) ? objectMeta.columns : [];
    const explicitColumns = buildColumnList(columns);
    const selectColumns = buildColumnProjection(columns);
    const businessKeys = Array.isArray(objectMeta.businessKeys) ? objectMeta.businessKeys.filter(Boolean) : [];
    const trackedColumns = Array.isArray(objectMeta.trackedColumns) && objectMeta.trackedColumns.length
        ? objectMeta.trackedColumns
        : columns;
    const businessKeySourceLabel = objectMeta.keySource === "primary_keys"
        ? "verified primary key"
        : objectMeta.keySource === "none"
            ? "manual review required"
            : `inferred key (${objectMeta.keySource})`;

    if (!columns.length) {
        return [
            `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
            `/* Source column metadata for ${sourceObject} was unavailable. Review the object before enabling recurring execution. */`,
        ].join("\n");
    }

    if (!businessKeys.length) {
        return [
            `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
            `/*`,
            `No stable business key could be verified for ${sourceObject}.`,
            `This starter avoids truncation and inserts only rows that do not already exist as an exact full-row match.`,
            `Review this step before scheduling recurring runs if the target must reflect in-place updates.`,
            `*/`,
            `INSERT INTO ${targetRef} (${explicitColumns})`,
            `SELECT`,
            `    ${selectColumns}`,
            `FROM ${sourceRef} AS src`,
            `WHERE NOT EXISTS (`,
            `    SELECT 1`,
            `    FROM ${targetRef} AS tgt`,
            `    WHERE ${buildFullRowMatchCondition(columns, "tgt", "src")}`,
            `);`,
        ].join("\n");
    }

    return [
        `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
        `/*`,
        `Non-destructive current-state sync for ${sourceObject}.`,
        `Business keys: ${businessKeys.join(", ")} (${businessKeySourceLabel}).`,
        `This starter updates existing rows and inserts missing rows without truncating ${targetObject}.`,
        `*/`,
        `UPDATE ${targetRef} AS tgt`,
        `   SET ${buildUpdateAssignments(trackedColumns, "src")}`,
        `  FROM ${sourceRef} AS src`,
        ` WHERE ${buildJoinCondition(businessKeys)}`,
        `   AND (`,
        `        ${buildChangeDetectionCondition(trackedColumns, "tgt", "src")}`,
        `   );`,
        `INSERT INTO ${targetRef} (${explicitColumns})`,
        `SELECT`,
        `    ${selectColumns}`,
        `FROM ${sourceRef} AS src`,
        `WHERE NOT EXISTS (`,
        `    SELECT 1`,
        `    FROM ${targetRef} AS tgt`,
        `    WHERE ${buildJoinCondition(businessKeys)}`,
        `);`,
    ].join("\n");
}

function buildFullRefreshStepSql(sourceSchema, targetSchema, objectMeta, targetObject) {
    const sourceObject = objectMeta.name;
    const sourceRef = qualifyObject(sourceSchema, sourceObject);
    const targetRef = qualifyObject(targetSchema, targetObject);
    const columns = Array.isArray(objectMeta.columns) ? objectMeta.columns : [];

    if (!columns.length) {
        return [
            `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
            `/* Source column metadata for ${sourceObject} was unavailable. Review this full-refresh step before running it. */`,
        ].join("\n");
    }

    return [
        `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
        `TRUNCATE TABLE ${targetRef} CASCADE;`,
        `INSERT INTO ${targetRef} (${buildColumnList(columns)})`,
        `SELECT`,
        `    ${buildColumnProjection(columns)}`,
        `FROM ${sourceRef};`,
    ].join("\n");
}

function buildHistoryStepSql(sourceSchema, targetSchema, objectMeta, targetObject) {
    const sourceObject = objectMeta.name;
    const sourceRef = qualifyObject(sourceSchema, sourceObject);
    const targetRef = qualifyObject(targetSchema, targetObject);
    const explicitColumns = buildColumnList(objectMeta.columns);
    const selectColumns = buildColumnProjection(objectMeta.columns);

    return [
        `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
        `ALTER TABLE ${targetRef}`,
        `    ADD COLUMN IF NOT EXISTS snapshot_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,`,
        `    ADD COLUMN IF NOT EXISTS snapshot_batch_id TEXT;`,
        `INSERT INTO ${targetRef} (${explicitColumns}, snapshot_at, snapshot_batch_id)`,
        `SELECT`,
        `    ${selectColumns},`,
        `    CURRENT_TIMESTAMP AS snapshot_at,`,
        `    to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') AS snapshot_batch_id`,
        `FROM ${sourceRef} AS src;`,
    ].join("\n");
}

function buildScd2StepSql(sourceSchema, targetSchema, objectMeta, targetObject) {
    const sourceObject = objectMeta.name;
    const sourceRef = qualifyObject(sourceSchema, sourceObject);
    const targetRef = qualifyObject(targetSchema, targetObject);
    const explicitColumns = buildColumnList(objectMeta.columns);
    const selectColumns = buildColumnProjection(objectMeta.columns);
    const businessKeys = objectMeta.businessKeys;
    const trackedColumns = objectMeta.trackedColumns;
    const businessKeyComment = businessKeys.length
        ? businessKeys.join(", ")
        : "No stable business key could be inferred";
    const businessKeySourceLabel = objectMeta.keySource === "primary_keys"
        ? "verified primary key"
        : objectMeta.keySource === "none"
            ? "manual review required"
            : `inferred key (${objectMeta.keySource})`;
    const joinCondition = buildJoinCondition(businessKeys);
    const changeHashSql = buildHashExpression(trackedColumns, "src");

    if (!businessKeys.length) {
        return [
            `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING DEFAULTS);`,
            `ALTER TABLE ${targetRef}`,
            `    ADD COLUMN IF NOT EXISTS scd_surrogate_key BIGSERIAL,`,
            `    ADD COLUMN IF NOT EXISTS valid_from TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,`,
            `    ADD COLUMN IF NOT EXISTS valid_to TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',`,
            `    ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE,`,
            `    ADD COLUMN IF NOT EXISTS change_hash TEXT;`,
            ``,
            `/*`,
            `No primary key could be verified or inferred for ${sourceObject}.`,
            `Review source metadata and replace this scaffold with a business-key-aware SCD Type 2 merge before enabling recurring execution.`,
            `*/`,
            `INSERT INTO ${targetRef} (${explicitColumns}, valid_from, valid_to, is_current, change_hash)`,
            `SELECT`,
            `    ${selectColumns},`,
            `    CURRENT_TIMESTAMP AS valid_from,`,
            `    TIMESTAMP '9999-12-31 00:00:00' AS valid_to,`,
            `    TRUE AS is_current,`,
            `    ${buildHashExpression(objectMeta.columns, "src")} AS change_hash`,
            `FROM ${sourceRef} AS src`,
            `WHERE NOT EXISTS (SELECT 1 FROM ${targetRef} AS tgt WHERE tgt.is_current = TRUE);`,
        ].join("\n");
    }

    return [
        `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING DEFAULTS);`,
        `ALTER TABLE ${targetRef}`,
        `    ADD COLUMN IF NOT EXISTS scd_surrogate_key BIGSERIAL,`,
        `    ADD COLUMN IF NOT EXISTS valid_from TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,`,
        `    ADD COLUMN IF NOT EXISTS valid_to TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',`,
        `    ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE,`,
        `    ADD COLUMN IF NOT EXISTS change_hash TEXT;`,
        ``,
        `/*`,
        `Business keys for ${sourceObject}: ${businessKeyComment} (${businessKeySourceLabel}).`,
        `Review the generated comparison columns before moving this starter into production scheduling.`,
        `*/`,
        `WITH source_prepared AS (`,
        `    SELECT`,
        `        ${selectColumns},`,
        `        ${changeHashSql} AS change_hash`,
        `    FROM ${sourceRef} AS src`,
        `),`,
        `changed_rows AS (`,
        `    SELECT src.*`,
        `    FROM source_prepared AS src`,
        `    LEFT JOIN ${targetRef} AS tgt`,
        `      ON tgt.is_current = TRUE`,
        `     AND ${joinCondition}`,
        `    WHERE tgt.scd_surrogate_key IS NULL`,
        `       OR tgt.change_hash IS DISTINCT FROM src.change_hash`,
        `),`,
        `closed_current AS (`,
        `    UPDATE ${targetRef} AS tgt`,
        `       SET valid_to = CURRENT_TIMESTAMP,`,
        `           is_current = FALSE`,
        `      FROM changed_rows AS src`,
        `     WHERE tgt.is_current = TRUE`,
        `       AND ${joinCondition}`,
        `    RETURNING tgt.scd_surrogate_key`,
        `)`,
        `INSERT INTO ${targetRef} (${explicitColumns}, valid_from, valid_to, is_current, change_hash)`,
        `SELECT`,
        `    ${selectColumns},`,
        `    CURRENT_TIMESTAMP AS valid_from,`,
        `    TIMESTAMP '9999-12-31 00:00:00' AS valid_to,`,
        `    TRUE AS is_current,`,
        `    src.change_hash`,
        `FROM changed_rows AS src;`,
    ].join("\n");
}

function buildStarterSteps({ sourceSchema, targetSchema, objects, templateId, tableSuffix }) {
    const steps = [
        {
            step_name: `Prepare target schema ${targetSchema}`,
            step_type: "sql",
            sql_text: `CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(targetSchema)};`,
            is_active: true,
        },
    ];

    objects.forEach((item) => {
        const targetObject = appendObjectSuffix(item.name, tableSuffix);
        let stepName = `Build ${targetObject}`;
        let sqlText = buildCoreStepSql(sourceSchema, targetSchema, item, targetObject);

        if (templateId === "full_refresh") {
            stepName = `Full refresh ${targetObject}`;
            sqlText = buildFullRefreshStepSql(sourceSchema, targetSchema, item, targetObject);
        }

        if (templateId === "history") {
            stepName = `Snapshot ${item.name} into ${targetObject}`;
            sqlText = buildHistoryStepSql(sourceSchema, targetSchema, item, targetObject);
        }

        if (templateId === "scd2") {
            stepName = `Create SCD2 scaffold for ${item.name}`;
            sqlText = buildScd2StepSql(sourceSchema, targetSchema, item, targetObject);
        }

        steps.push({
            step_name: stepName,
            step_type: "sql",
            sql_text: sqlText,
            is_active: true,
        });
    });

    return steps;
}

function buildSchemaStarterDraft({
    connectionId,
    databaseName,
    schemaName,
    selectedObject,
    availableObjects,
    starterConfig,
}) {
    const template = getStarterTemplate(starterConfig.templateId);
    const sourceObjects = selectStarterObjects(availableObjects, selectedObject, starterConfig);
    const targetSchema = (starterConfig.targetSchema || getTemplateDefaultTargetSchema(schemaName, template)).trim();
    const tableSuffix = starterConfig.tableSuffix || template.tableSuffix;
    const objectCount = sourceObjects.length;
    const sourceLabel = starterConfig.scope === "current-object"
        ? (selectedObject || sourceObjects[0]?.name || "")
        : `${schemaName}.*`;
    const targetLabel = starterConfig.scope === "current-object"
        ? `${targetSchema}.${appendObjectSuffix(sourceObjects[0]?.name || selectedObject || "", tableSuffix)}`
        : `${targetSchema}.*`;

    const header =
        connectionId && databaseName && schemaName && targetSchema && objectCount
            ? {
                name: (starterConfig.pipelineName || buildStarterPipelineName(schemaName, template.label)).trim(),
                description: (
                    starterConfig.pipelineDescription ||
                    buildStarterDescription(
                        template.id,
                        schemaName,
                        objectCount,
                        targetSchema,
                        selectedObject,
                        starterConfig.scope
                    )
                ).trim(),
                connection_id: Number(connectionId),
                database_name: databaseName,
                schema_name: schemaName,
                source_object: sourceLabel,
                target_object: targetLabel,
            }
            : null;

    return {
        template,
        sourceObjects,
        header,
        steps:
            header && targetSchema
                ? buildStarterSteps({
                    sourceSchema: schemaName,
                    targetSchema,
                    objects: sourceObjects,
                    templateId: template.id,
                    tableSuffix,
                })
                : [],
        targetSchema,
    };
}

async function fetchStarterObjectMetadata(connectionId, databaseName, schemaName, objects) {
    const results = await Promise.all(objects.map(async (item) => {
        const [details, relationships] = await Promise.all([
            fetchObjectDetails(connectionId, databaseName, schemaName, item.name),
            fetchRelationships(connectionId, databaseName, schemaName, item.name).catch(() => null),
        ]);

        return normalizeStarterObjectMetadata(item, details, relationships);
    }));

    return results;
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
const STEP_TEMPLATE_OPTIONS = [
    {
        id: "dedup",
        label: "Deduplication (DISTINCT)",
        summary: "Create and load deduplicated rows into the target object.",
    },
    {
        id: "history",
        label: "History Only",
        summary: "Append snapshot rows each run with timestamp markers.",
    },
    {
        id: "scd1",
        label: "SCD Type I (Overwrite)",
        summary: "Keep only current state by replacing target content.",
    },
    {
        id: "scd2",
        label: "SCD Type II (Snapshot Versioning)",
        summary: "Close previous versions and insert a fresh current snapshot.",
    },
    {
        id: "scd3",
        label: "SCD Type III (Current + Previous Snapshot)",
        summary: "Track both current and previous snapshot timestamps.",
    },
];

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

function normalizeIdentifierValue(value = "") {
    return String(value || "").trim().replace(/^"+|"+$/g, "");
}

function normalizeMappingColumn(column = {}) {
    return {
        source_column: normalizeIdentifierValue(column.source_column || ""),
        target_column: normalizeIdentifierValue(column.target_column || ""),
        data_type: String(column.data_type || "text").trim() || "text",
        is_nullable: column.is_nullable !== false,
        include: column.include !== false,
    };
}

function buildDefaultPipelineMapping(schemaName, selectedObject, targetObject) {
    const parsedTarget = parseSchemaObject(targetObject || "");
    return {
        source: {
            schema: normalizeIdentifierValue(schemaName || ""),
            object: normalizeIdentifierValue(selectedObject || ""),
        },
        target: {
            schema: normalizeIdentifierValue(parsedTarget.schema || ""),
            object: normalizeIdentifierValue(parsedTarget.object || ""),
            mode: parsedTarget.object ? "existing" : "create_new",
        },
        columns: [],
    };
}

function normalizePipelineMappingConfig(mappingConfig, schemaName, sourceObject, targetObject) {
    const fallback = buildDefaultPipelineMapping(schemaName, sourceObject, targetObject);
    const source = mappingConfig?.source || {};
    const target = mappingConfig?.target || {};

    return {
        source: {
            schema: normalizeIdentifierValue(source.schema || fallback.source.schema),
            object: normalizeIdentifierValue(source.object || fallback.source.object),
        },
        target: {
            schema: normalizeIdentifierValue(target.schema || fallback.target.schema),
            object: normalizeIdentifierValue(target.object || fallback.target.object),
            mode: target.mode === "create_new" ? "create_new" : "existing",
        },
        columns: Array.isArray(mappingConfig?.columns)
            ? mappingConfig.columns.map((column) => normalizeMappingColumn(column))
            : [],
    };
}

function syncMappingConfigWithHeader(mappingConfig, schemaName, sourceObject, targetObject) {
    const normalized = normalizePipelineMappingConfig(mappingConfig, schemaName, sourceObject, targetObject);
    const parsedSource = parseSchemaObject(sourceObject || "");
    const parsedTarget = parseSchemaObject(targetObject || "");

    return {
        ...normalized,
        source: {
            schema: normalizeIdentifierValue(parsedSource.schema || schemaName || normalized.source.schema),
            object: normalizeIdentifierValue(parsedSource.object || sourceObject || normalized.source.object),
        },
        target: {
            ...normalized.target,
            schema: normalizeIdentifierValue(parsedTarget.schema || normalized.target.schema),
            object: normalizeIdentifierValue(parsedTarget.object || normalized.target.object),
        },
    };
}

function buildMappingColumnsFromMetadata(sourceColumns = [], targetColumns = []) {
    const targetByName = new Map(
        targetColumns.map((column) => [String(column.column_name || "").toLowerCase(), column])
    );
    const matchedTargets = new Set();

    const mappedColumns = sourceColumns.map((column) => {
        const targetMatch = targetByName.get(String(column.column_name || "").toLowerCase());
        if (targetMatch) {
            matchedTargets.add(String(targetMatch.column_name || "").toLowerCase());
        }

        return normalizeMappingColumn({
            source_column: column.column_name,
            target_column: targetMatch?.column_name || column.column_name,
            data_type: targetMatch?.data_type || column.data_type || "text",
            is_nullable: (targetMatch?.is_nullable || column.is_nullable) !== "NO",
            include: true,
        });
    });

    targetColumns.forEach((column) => {
        const targetKey = String(column.column_name || "").toLowerCase();
        if (matchedTargets.has(targetKey)) {
            return;
        }

        mappedColumns.push(normalizeMappingColumn({
            source_column: "",
            target_column: column.column_name,
            data_type: column.data_type || "text",
            is_nullable: column.is_nullable !== "NO",
            include: true,
        }));
    });

    return mappedColumns;
}

const MAPPING_CANVAS_NODE_HEIGHT = 104;
const MAPPING_CANVAS_NODE_GAP = 16;
const MAPPING_CANVAS_VERTICAL_PADDING = 12;
const MAPPING_CANVAS_CONNECTOR_WIDTH = 220;
const MAPPING_MODAL_BOX_WIDTH = 320;
const MAPPING_MODAL_BOX_HEADER_HEIGHT = 58;
const MAPPING_MODAL_BOX_ROW_HEIGHT = 36;
const MAPPING_MODAL_STAGE_WIDTH = 1380;
const MAPPING_MODAL_TARGET_BOX_MAX_WIDTH = 560;

function buildCanvasTargetColumns(mappingColumns = [], targetDetails = null) {
    if (Array.isArray(mappingColumns) && mappingColumns.length) {
        return mappingColumns.map((column) => normalizeMappingColumn(column));
    }

    if (Array.isArray(targetDetails?.columns) && targetDetails.columns.length) {
        return targetDetails.columns.map((column) => normalizeMappingColumn({
            source_column: "",
            target_column: column.column_name,
            data_type: column.data_type || "text",
            is_nullable: column.is_nullable !== "NO",
            include: true,
        }));
    }

    return [];
}

function getMappingInputWidthCh(value = "", fallback = 12, ceiling = 28) {
    const contentLength = String(value || "").trim().length;
    return Math.min(Math.max(contentLength + 2, fallback), ceiling);
}

function getMappingTargetBoxWidth(mappingColumns = []) {
    const maxTargetCh = mappingColumns.reduce(
        (largest, column) => Math.max(largest, getMappingInputWidthCh(column.target_column, 14, 30)),
        14
    );
    const maxTypeCh = mappingColumns.reduce(
        (largest, column) => Math.max(largest, getMappingInputWidthCh(column.data_type, 10, 18)),
        10
    );
    const estimatedWidth = 164 + (maxTargetCh * 8) + (maxTypeCh * 7);
    return Math.min(Math.max(estimatedWidth, MAPPING_MODAL_BOX_WIDTH), MAPPING_MODAL_TARGET_BOX_MAX_WIDTH);
}

function getMappingCanvasHeight(sourceCount, targetCount) {
    const laneCount = Math.max(sourceCount, targetCount, 1);
    return (laneCount * MAPPING_CANVAS_NODE_HEIGHT)
        + (Math.max(laneCount - 1, 0) * MAPPING_CANVAS_NODE_GAP)
        + (MAPPING_CANVAS_VERTICAL_PADDING * 2);
}

function getMappingNodeCenterY(index) {
    return MAPPING_CANVAS_VERTICAL_PADDING
        + (index * (MAPPING_CANVAS_NODE_HEIGHT + MAPPING_CANVAS_NODE_GAP))
        + (MAPPING_CANVAS_NODE_HEIGHT / 2);
}

function buildMappingCurvePath(startX, startY, endX, endY) {
    const controlOffset = Math.max((endX - startX) * 0.35, 36);
    return [
        `M ${startX} ${startY}`,
        `C ${startX + controlOffset} ${startY}, ${endX - controlOffset} ${endY}, ${endX} ${endY}`,
    ].join(" ");
}

function getMappingModalBoxHeight(columnCount) {
    const safeCount = Math.max(columnCount, 1);
    return MAPPING_MODAL_BOX_HEADER_HEIGHT + (safeCount * MAPPING_MODAL_BOX_ROW_HEIGHT) + 24;
}

function getMappingModalRowCenterY(boxY, index) {
    return boxY + MAPPING_MODAL_BOX_HEADER_HEIGHT + 12 + (index * MAPPING_MODAL_BOX_ROW_HEIGHT) + (MAPPING_MODAL_BOX_ROW_HEIGHT / 2);
}

function parseSchemaObject(value = "") {
    const cleaned = normalizeIdentifierValue(value);
    if (!cleaned) {
        return { schema: "", object: "" };
    }

    const parts = cleaned.split(".", 2);
    if (parts.length === 2) {
        return {
            schema: normalizeIdentifierValue(parts[0]),
            object: normalizeIdentifierValue(parts[1]),
        };
    }

    return {
        schema: "",
        object: cleaned,
    };
}

function formatQualifiedObjectName(schemaName = "", objectName = "") {
    const schema = normalizeIdentifierValue(schemaName);
    const object = normalizeIdentifierValue(objectName);
    if (!object) {
        return "";
    }
    return schema ? `${schema}.${object}` : object;
}

function isWildcardIdentifier(value = "") {
    const cleaned = normalizeIdentifierValue(value);
    return !cleaned || cleaned.includes("*");
}

function resolveQuickTemplateContext({ pipelineForm, schemaName, selectedObject }) {
    const sourceFromForm = parseSchemaObject(pipelineForm?.source_object || "");
    const targetFromForm = parseSchemaObject(pipelineForm?.target_object || "");
    const selectedObjectName = normalizeIdentifierValue(selectedObject);
    const pipelineSchemaName = normalizeIdentifierValue(pipelineForm?.schema_name);

    const sourceSchema =
        sourceFromForm.schema ||
        pipelineSchemaName ||
        normalizeIdentifierValue(schemaName) ||
        "src";

    let sourceObject = sourceFromForm.object || selectedObjectName;
    if (isWildcardIdentifier(sourceObject)) {
        sourceObject = sourceFromForm.object || selectedObjectName || "source_table";
    }

    let targetSchema = targetFromForm.schema;
    if (!targetSchema) {
        targetSchema =
            pipelineSchemaName && pipelineSchemaName.toLowerCase() !== sourceSchema.toLowerCase()
                ? pipelineSchemaName
                : sourceSchema;
    }

    let targetObject = targetFromForm.object;
    if (isWildcardIdentifier(targetObject)) {
        targetObject = "";
    }

    return {
        sourceSchema,
        sourceObject,
        targetSchema,
        targetObject,
    };
}

function buildQuickStepTemplate(templateId, sourceSchema, targetSchema, sourceObject, targetObject) {
    const sourceName = sourceObject || "source_table";
    const sourceSchemaName = sourceSchema || "src";
    const targetSchemaName = targetSchema || sourceSchemaName;
    const defaultTargetByTemplate = {
        dedup: `${sourceName}_dedup`,
        history: `${sourceName}_hist`,
        scd1: `${sourceName}_cur`,
        scd2: `${sourceName}_scd2`,
        scd3: `${sourceName}_scd3`,
    };
    const resolvedTarget = targetObject || defaultTargetByTemplate[templateId] || `${sourceName}_target`;

    const sourceRef = qualifyObject(sourceSchemaName, sourceName);
    const targetRef = qualifyObject(targetSchemaName, resolvedTarget);

    if (templateId === "history") {
        return {
            stepName: `History Snapshot ${sourceName}`,
            sqlText: [
                `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
                `ALTER TABLE ${targetRef}`,
                `    ADD COLUMN IF NOT EXISTS snapshot_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,`,
                `    ADD COLUMN IF NOT EXISTS snapshot_batch_id TEXT;`,
                `INSERT INTO ${targetRef}`,
                `SELECT src.*,`,
                `       CURRENT_TIMESTAMP AS snapshot_at,`,
                `       to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') AS snapshot_batch_id`,
                `FROM ${sourceRef} AS src;`,
            ].join("\n"),
        };
    }

    if (templateId === "scd1") {
        return {
            stepName: `SCD Type I Refresh ${sourceName}`,
            sqlText: [
                `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
                `TRUNCATE TABLE ${targetRef} CASCADE;`,
                `INSERT INTO ${targetRef}`,
                `SELECT src.*`,
                `FROM ${sourceRef} AS src;`,
            ].join("\n"),
        };
    }

    if (templateId === "scd2") {
        return {
            stepName: `SCD Type II Snapshot ${sourceName}`,
            sqlText: [
                `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING DEFAULTS);`,
                `ALTER TABLE ${targetRef}`,
                `    ADD COLUMN IF NOT EXISTS valid_from TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,`,
                `    ADD COLUMN IF NOT EXISTS valid_to TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',`,
                `    ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE,`,
                `    ADD COLUMN IF NOT EXISTS change_hash TEXT;`,
                `UPDATE ${targetRef}`,
                `SET valid_to = CURRENT_TIMESTAMP,`,
                `    is_current = FALSE`,
                `WHERE is_current = TRUE;`,
                `INSERT INTO ${targetRef}`,
                `SELECT src.*,`,
                `       CURRENT_TIMESTAMP AS valid_from,`,
                `       TIMESTAMP '9999-12-31 00:00:00' AS valid_to,`,
                `       TRUE AS is_current,`,
                `       md5(row_to_json(src)::text) AS change_hash`,
                `FROM ${sourceRef} AS src;`,
            ].join("\n"),
        };
    }

    if (templateId === "scd3") {
        return {
            stepName: `SCD Type III Snapshot ${sourceName}`,
            sqlText: [
                `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING DEFAULTS);`,
                `ALTER TABLE ${targetRef}`,
                `    ADD COLUMN IF NOT EXISTS current_snapshot_at TIMESTAMP WITHOUT TIME ZONE,`,
                `    ADD COLUMN IF NOT EXISTS previous_snapshot_at TIMESTAMP WITHOUT TIME ZONE;`,
                `UPDATE ${targetRef}`,
                `SET previous_snapshot_at = current_snapshot_at,`,
                `    current_snapshot_at = CURRENT_TIMESTAMP;`,
                `INSERT INTO ${targetRef}`,
                `SELECT src.*,`,
                `       CURRENT_TIMESTAMP AS current_snapshot_at,`,
                `       NULL::TIMESTAMP WITHOUT TIME ZONE AS previous_snapshot_at`,
                `FROM ${sourceRef} AS src;`,
            ].join("\n"),
        };
    }

    return {
        stepName: `Deduplicate ${sourceName}`,
        sqlText: [
            `CREATE TABLE IF NOT EXISTS ${targetRef} (LIKE ${sourceRef} INCLUDING ALL);`,
            `TRUNCATE TABLE ${targetRef} CASCADE;`,
            `INSERT INTO ${targetRef}`,
            `SELECT DISTINCT *`,
            `FROM ${sourceRef};`,
        ].join("\n"),
    };
}

export default function PipelineBuilder({
    sessionKey,
    connectionId,
    databaseName,
    schemaName,
    selectedObject,
    availableObjects = [],
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
    const [starterConfig, setStarterConfig] = useState(() => normalizePipelineSession(
        pipelineSession,
        connectionId,
        databaseName,
        schemaName,
        selectedObject
    ).starterConfig);
    const [creatingStarter, setCreatingStarter] = useState(false);
    const [starterMetadataStatus, setStarterMetadataStatus] = useState("");

    const canCreatePipeline = useMemo(() => {
        return !!pipelineForm.name?.trim() && !!pipelineForm.connection_id;
    }, [pipelineForm]);

    const starterDraft = useMemo(() => buildSchemaStarterDraft({
        connectionId,
        databaseName,
        schemaName,
        selectedObject,
        availableObjects,
        starterConfig,
    }), [connectionId, databaseName, schemaName, selectedObject, availableObjects, starterConfig]);

    const starterObjectPreview = useMemo(() => {
        return starterDraft.sourceObjects.slice(0, 6).map((item) => item.name);
    }, [starterDraft]);

    const canCreateStarterPipeline = useMemo(() => {
        return !!starterDraft.header && starterDraft.steps.length > 1;
    }, [starterDraft]);

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
        setStarterConfig(next.starterConfig);
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
        setPipelineForm((prev) => {
            const nextMapping = syncMappingConfigWithHeader(
                prev.mapping_config,
                prev.schema_name,
                prev.source_object,
                prev.target_object
            );
            const currentMapping = normalizePipelineMappingConfig(
                prev.mapping_config,
                prev.schema_name,
                prev.source_object,
                prev.target_object
            );

            if (JSON.stringify(nextMapping) === JSON.stringify(currentMapping)) {
                return prev;
            }

            return {
                ...prev,
                mapping_config: nextMapping,
            };
        });
    }, [pipelineForm.schema_name, pipelineForm.source_object, pipelineForm.target_object]);

    useEffect(() => {
        const nextScope = getDefaultStarterScope(selectedObject);

        setStarterConfig((prev) => {
            if (prev.scope === nextScope) {
                return prev;
            }

            return {
                ...prev,
                scope: nextScope,
                pipelineDescription: buildStarterDescription(
                    prev.templateId,
                    schemaName,
                    nextScope === "current-object" ? 1 : 0,
                    prev.targetSchema,
                    selectedObject,
                    nextScope
                ),
            };
        });
    }, [schemaName, selectedObject]);

    const parsedTargetForPicker = parseSchemaObject(pipelineForm.target_object || "");
    const targetSchemaForPicker = normalizeIdentifierValue(
        pipelineForm.mapping_config?.target?.schema || parsedTargetForPicker.schema || ""
    );

    useEffect(() => {
        let cancelled = false;
        const activeConnectionId = Number(pipelineForm.connection_id || connectionId || 0);
        const activeDatabaseName = pipelineForm.database_name || databaseName || "";

        if (!activeConnectionId || !activeDatabaseName) {
            setTargetSchemaOptions([]);
            setTargetPickerError("");
            return undefined;
        }

        async function loadTargetSchemas() {
            try {
                setTargetPickerError("");
                const result = await fetchSchemas(activeConnectionId, activeDatabaseName);
                if (!cancelled) {
                    setTargetSchemaOptions(Array.isArray(result.schemas) ? result.schemas : []);
                }
            } catch (err) {
                if (!cancelled) {
                    setTargetSchemaOptions([]);
                    setTargetPickerError(err.message || "Failed to load target schemas.");
                }
            }
        }

        loadTargetSchemas();
        return () => {
            cancelled = true;
        };
    }, [connectionId, databaseName, pipelineForm.connection_id, pipelineForm.database_name]);

    useEffect(() => {
        let cancelled = false;
        const activeConnectionId = Number(pipelineForm.connection_id || connectionId || 0);
        const activeDatabaseName = pipelineForm.database_name || databaseName || "";

        if (!activeConnectionId || !activeDatabaseName || !targetSchemaForPicker) {
            setTargetObjectOptions([]);
            setTargetPickerLoading(false);
            return undefined;
        }

        async function loadTargetObjects() {
            try {
                setTargetPickerLoading(true);
                setTargetPickerError("");
                const result = await fetchObjects(activeConnectionId, activeDatabaseName, targetSchemaForPicker);
                if (!cancelled) {
                    setTargetObjectOptions(Array.isArray(result.objects) ? result.objects : []);
                }
            } catch (err) {
                if (!cancelled) {
                    setTargetObjectOptions([]);
                    setTargetPickerError(err.message || "Failed to load target objects.");
                }
            } finally {
                if (!cancelled) {
                    setTargetPickerLoading(false);
                }
            }
        }

        loadTargetObjects();
        return () => {
            cancelled = true;
        };
    }, [connectionId, databaseName, pipelineForm.connection_id, pipelineForm.database_name, targetSchemaForPicker]);

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
            starterConfig,
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
        starterConfig,
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
                mapping_config: normalizePipelineMappingConfig(
                    data.mapping_config,
                    data.schema_name || "",
                    data.source_object || "",
                    data.target_object || ""
                ),
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
                mapping_config: normalizePipelineMappingConfig(
                    pipelineForm.mapping_config,
                    pipelineForm.schema_name,
                    pipelineForm.source_object,
                    pipelineForm.target_object
                ),
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

    async function handleCreateStarterPipeline() {
        if (!canCreateStarterPipeline || !starterDraft.header) {
            setError("Choose a schema starter pattern and make sure the current schema has at least one object.");
            return;
        }

        try {
            setCreatingStarter(true);
            setError("");
            setMessage("");
            setStarterMetadataStatus("Loading source metadata...");

            const objectMetadata = await fetchStarterObjectMetadata(
                connectionId,
                databaseName,
                schemaName,
                starterDraft.sourceObjects
            );

            const metadataAwareSteps = buildStarterSteps({
                sourceSchema: schemaName,
                targetSchema: starterDraft.targetSchema,
                objects: objectMetadata,
                templateId: starterDraft.template.id,
                tableSuffix: starterConfig.tableSuffix || starterDraft.template.tableSuffix,
            });

            const inferredKeyTables = objectMetadata
                .filter((item) => item.keySource !== "primary_keys")
                .map((item) => `${item.name} (${item.keySource === "none" ? "manual review" : "inferred key"})`);

            const created = await createPipeline(starterDraft.header);
            setStarterMetadataStatus("Writing starter steps...");
            const seededPipeline = await importPipelineSteps(created.id, {
                steps: metadataAwareSteps,
            });

            setSelectedPipeline(seededPipeline);
            setSelectedPipelineId(seededPipeline.id);
            setSelectedRun(null);
            setRuns([]);
            setActiveSubtab("builder");
            setStepForm(buildDefaultStepForm());
            setPipelineForm({
                name: seededPipeline.name || starterDraft.header.name,
                description: seededPipeline.description || starterDraft.header.description || "",
                connection_id: String(seededPipeline.connection_id || starterDraft.header.connection_id || ""),
                database_name: seededPipeline.database_name || starterDraft.header.database_name || "",
                schema_name: seededPipeline.schema_name || starterDraft.header.schema_name || "",
                source_object: seededPipeline.source_object || starterDraft.header.source_object || "",
                target_object: seededPipeline.target_object || starterDraft.header.target_object || "",
            });

            await loadPipelines();

            setMessage(
                inferredKeyTables.length
                    ? `Created ${starterDraft.template.label} starter pipeline for ${starterDraft.sourceObjects.length} object(s) in ${schemaName}. Review inferred keys for: ${inferredKeyTables.join(", ")}.`
                    : `Created ${starterDraft.template.label} starter pipeline for ${starterDraft.sourceObjects.length} object(s) in ${schemaName} using source metadata.`
            );
        } catch (err) {
            setError(err.message || "Failed to create starter pipeline");
        } finally {
            setStarterMetadataStatus("");
            setCreatingStarter(false);
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
                mapping_config: normalizePipelineMappingConfig(
                    pipelineForm.mapping_config,
                    pipelineForm.schema_name,
                    pipelineForm.source_object,
                    pipelineForm.target_object
                ),
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
                is_active: true,
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
                    is_active: true,
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
                    is_active: true,
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
                        is_active: true,
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
                    schema: selectedPipeline?.schema_name || schemaName || "src",
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
    const [selectedStepTemplateId, setSelectedStepTemplateId] = useState("dedup");
    const [mappingSourceDetails, setMappingSourceDetails] = useState(null);
    const [mappingTargetDetails, setMappingTargetDetails] = useState(null);
    const [mappingMetadataLoading, setMappingMetadataLoading] = useState(false);
    const [mappingMetadataError, setMappingMetadataError] = useState("");
    const [selectedMappingSource, setSelectedMappingSource] = useState("");
    const [aiMappingLoading, setAiMappingLoading] = useState(false);
    const [targetSchemaOptions, setTargetSchemaOptions] = useState([]);
    const [targetObjectOptions, setTargetObjectOptions] = useState([]);
    const [targetPickerLoading, setTargetPickerLoading] = useState(false);
    const [targetPickerError, setTargetPickerError] = useState("");
    const [showMappingCanvas, setShowMappingCanvas] = useState(false);
    const [mappingCanvasTf, setMappingCanvasTf] = useState({ x: 0, y: 0, scale: 1 });
    const [mappingBoxPositions, setMappingBoxPositions] = useState({
        source: { x: 120, y: 120 },
        target: { x: 860, y: 120 },
    });
    const mappingViewportRef = useRef(null);
    const mappingPanRef = useRef({ active: false, startX: 0, startY: 0, originX: 0, originY: 0 });
    const mappingBoxDragRef = useRef({ box: "", startX: 0, startY: 0, originX: 0, originY: 0, active: false });

    const normalizedMappingConfig = useMemo(() => normalizePipelineMappingConfig(
        pipelineForm.mapping_config,
        pipelineForm.schema_name,
        pipelineForm.source_object,
        pipelineForm.target_object
    ), [
        pipelineForm.mapping_config,
        pipelineForm.schema_name,
        pipelineForm.source_object,
        pipelineForm.target_object,
    ]);

    const selectedTargetReference = useMemo(() => {
        const parsedTarget = parseSchemaObject(pipelineForm.target_object || "");
        return {
            schema: normalizeIdentifierValue(normalizedMappingConfig.target.schema || parsedTarget.schema || ""),
            object: normalizeIdentifierValue(normalizedMappingConfig.target.object || parsedTarget.object || ""),
        };
    }, [normalizedMappingConfig.target.object, normalizedMappingConfig.target.schema, pipelineForm.target_object]);

    const hasUnsavedPipelineDefinition = useMemo(() => {
        if (!selectedPipelineId || !selectedPipeline) {
            return false;
        }

        const currentSnapshot = JSON.stringify({
            name: pipelineForm.name || "",
            description: pipelineForm.description || "",
            connection_id: Number(pipelineForm.connection_id || 0),
            database_name: pipelineForm.database_name || "",
            schema_name: pipelineForm.schema_name || "",
            source_object: pipelineForm.source_object || "",
            target_object: pipelineForm.target_object || "",
            mapping_config: normalizedMappingConfig,
        });
        const savedSnapshot = JSON.stringify({
            name: selectedPipeline.name || "",
            description: selectedPipeline.description || "",
            connection_id: Number(selectedPipeline.connection_id || 0),
            database_name: selectedPipeline.database_name || "",
            schema_name: selectedPipeline.schema_name || "",
            source_object: selectedPipeline.source_object || "",
            target_object: selectedPipeline.target_object || "",
            mapping_config: normalizePipelineMappingConfig(
                selectedPipeline.mapping_config,
                selectedPipeline.schema_name || "",
                selectedPipeline.source_object || "",
                selectedPipeline.target_object || ""
            ),
        });

        return currentSnapshot !== savedSnapshot;
    }, [normalizedMappingConfig, pipelineForm, selectedPipeline, selectedPipelineId]);

    const selectedStepTemplateMeta = useMemo(() => {
        return STEP_TEMPLATE_OPTIONS.find((item) => item.id === selectedStepTemplateId) || STEP_TEMPLATE_OPTIONS[0];
    }, [selectedStepTemplateId]);

    const mappingSourceColumns = useMemo(() => {
        return Array.isArray(mappingSourceDetails?.columns) ? mappingSourceDetails.columns : [];
    }, [mappingSourceDetails]);

    const mappingCanvasTargets = useMemo(() => {
        return buildCanvasTargetColumns(normalizedMappingConfig.columns, mappingTargetDetails);
    }, [normalizedMappingConfig.columns, mappingTargetDetails]);

    const mappingSourceUsage = useMemo(() => {
        const counts = new Map();
        mappingCanvasTargets.forEach((column) => {
            if (!column.include || !column.source_column) {
                return;
            }
            const key = String(column.source_column).toLowerCase();
            counts.set(key, (counts.get(key) || 0) + 1);
        });
        return counts;
    }, [mappingCanvasTargets]);

    const mappingCanvasHeight = useMemo(() => {
        return getMappingCanvasHeight(mappingSourceColumns.length, mappingCanvasTargets.length);
    }, [mappingSourceColumns.length, mappingCanvasTargets.length]);

    const mappingSourceBoxHeight = useMemo(() => {
        return getMappingModalBoxHeight(mappingSourceColumns.length);
    }, [mappingSourceColumns.length]);

    const mappingTargetBoxHeight = useMemo(() => {
        return getMappingModalBoxHeight(mappingCanvasTargets.length);
    }, [mappingCanvasTargets.length]);

    const mappingTargetBoxWidth = useMemo(() => {
        return getMappingTargetBoxWidth(mappingCanvasTargets);
    }, [mappingCanvasTargets]);

    const mappingModalStageHeight = useMemo(() => {
        return Math.max(mappingSourceBoxHeight, mappingTargetBoxHeight) + 320;
    }, [mappingSourceBoxHeight, mappingTargetBoxHeight]);

    const mappingConnections = useMemo(() => {
        const sourceIndexByName = new Map(
            mappingSourceColumns.map((column, index) => [String(column.column_name || "").toLowerCase(), index])
        );

        return mappingCanvasTargets
            .map((column, targetIndex) => {
                const sourceKey = String(column.source_column || "").toLowerCase();
                if (!column.include || !sourceKey || !sourceIndexByName.has(sourceKey)) {
                    return null;
                }

                const sourceIndex = sourceIndexByName.get(sourceKey);
                return {
                    sourceIndex,
                    targetIndex,
                    sourceColumn: column.source_column,
                    targetColumn: column.target_column,
                };
            })
            .filter(Boolean);
    }, [mappingCanvasTargets, mappingSourceColumns]);

    const mappingModalConnections = useMemo(() => {
        return mappingConnections.map((connection) => {
            const startX = mappingBoxPositions.source.x + MAPPING_MODAL_BOX_WIDTH;
            const startY = getMappingModalRowCenterY(mappingBoxPositions.source.y, connection.sourceIndex);
            const endX = mappingBoxPositions.target.x;
            const endY = getMappingModalRowCenterY(mappingBoxPositions.target.y, connection.targetIndex);
            return {
                ...connection,
                path: buildMappingCurvePath(startX, startY, endX, endY),
                startX,
                startY,
                endX,
                endY,
            };
        });
    }, [mappingBoxPositions.source.x, mappingBoxPositions.source.y, mappingBoxPositions.target.x, mappingBoxPositions.target.y, mappingConnections]);

    useEffect(() => {
        if (!selectedMappingSource) {
            return;
        }
        const sourceStillExists = mappingSourceColumns.some(
            (column) => String(column.column_name || "").toLowerCase() === String(selectedMappingSource).toLowerCase()
        );
        if (!sourceStillExists) {
            setSelectedMappingSource("");
        }
    }, [mappingSourceColumns, selectedMappingSource]);

    const fitMappingCanvas = useCallback(() => {
        if (!mappingViewportRef.current) {
            return;
        }

        const viewportWidth = mappingViewportRef.current.clientWidth || 1100;
        const viewportHeight = mappingViewportRef.current.clientHeight || 620;
        const minX = Math.min(mappingBoxPositions.source.x, mappingBoxPositions.target.x);
        const minY = Math.min(mappingBoxPositions.source.y, mappingBoxPositions.target.y);
        const maxX = Math.max(
            mappingBoxPositions.source.x + MAPPING_MODAL_BOX_WIDTH,
            mappingBoxPositions.target.x + mappingTargetBoxWidth
        );
        const maxY = Math.max(
            mappingBoxPositions.source.y + mappingSourceBoxHeight,
            mappingBoxPositions.target.y + mappingTargetBoxHeight
        );
        const contentWidth = Math.max(maxX - minX, 680);
        const contentHeight = Math.max(maxY - minY, 280);
        const pad = 64;
        const scale = Math.min(
            (viewportWidth - pad * 2) / contentWidth,
            (viewportHeight - pad * 2) / contentHeight,
            1.2
        );

        setMappingCanvasTf({
            x: (viewportWidth - contentWidth * scale) / 2 - (minX * scale),
            y: (viewportHeight - contentHeight * scale) / 2 - (minY * scale),
            scale,
        });
    }, [mappingBoxPositions.source.x, mappingBoxPositions.source.y, mappingBoxPositions.target.x, mappingBoxPositions.target.y, mappingSourceBoxHeight, mappingTargetBoxHeight, mappingTargetBoxWidth]);

    const zoomMappingCanvas = useCallback((factor, anchor = null) => {
        setMappingCanvasTf((prev) => {
            const viewport = mappingViewportRef.current;
            const fallbackX = viewport ? viewport.clientWidth / 2 : 480;
            const fallbackY = viewport ? viewport.clientHeight / 2 : 300;
            const anchorX = anchor?.x ?? fallbackX;
            const anchorY = anchor?.y ?? fallbackY;
            const nextScale = Math.min(Math.max(prev.scale * factor, 0.22), 2.4);
            const canvasX = (anchorX - prev.x) / prev.scale;
            const canvasY = (anchorY - prev.y) / prev.scale;
            return {
                x: anchorX - canvasX * nextScale,
                y: anchorY - canvasY * nextScale,
                scale: nextScale,
            };
        });
    }, []);

    const handleMappingViewportWheel = useCallback((event) => {
        event.preventDefault();
        const rect = event.currentTarget.getBoundingClientRect();
        zoomMappingCanvas(event.deltaY < 0 ? 1.14 : (1 / 1.14), {
            x: event.clientX - rect.left,
            y: event.clientY - rect.top,
        });
    }, [zoomMappingCanvas]);

    const handleMappingViewportMouseDown = useCallback((event) => {
        if (event.button !== 0) {
            return;
        }
        if (event.target.closest(".pipeline-mapper-box") || event.target.closest(".pipeline-mapping-canvas-toolbar")) {
            return;
        }
        mappingPanRef.current = {
            active: true,
            startX: event.clientX,
            startY: event.clientY,
            originX: mappingCanvasTf.x,
            originY: mappingCanvasTf.y,
        };
    }, [mappingCanvasTf.x, mappingCanvasTf.y]);

    const beginMappingBoxDrag = useCallback((boxName, event) => {
        event.preventDefault();
        event.stopPropagation();
        mappingBoxDragRef.current = {
            box: boxName,
            startX: event.clientX,
            startY: event.clientY,
            originX: mappingBoxPositions[boxName].x,
            originY: mappingBoxPositions[boxName].y,
            active: true,
        };
    }, [mappingBoxPositions]);

    useEffect(() => {
        if (!showMappingCanvas) {
            return undefined;
        }

        const handleMouseMove = (event) => {
            if (mappingBoxDragRef.current.active) {
                const dx = (event.clientX - mappingBoxDragRef.current.startX) / Math.max(mappingCanvasTf.scale, 0.001);
                const dy = (event.clientY - mappingBoxDragRef.current.startY) / Math.max(mappingCanvasTf.scale, 0.001);
                const boxName = mappingBoxDragRef.current.box;
                setMappingBoxPositions((prev) => ({
                    ...prev,
                    [boxName]: {
                        x: Math.max(48, mappingBoxDragRef.current.originX + dx),
                        y: Math.max(48, mappingBoxDragRef.current.originY + dy),
                    },
                }));
                return;
            }

            if (mappingPanRef.current.active) {
                const dx = event.clientX - mappingPanRef.current.startX;
                const dy = event.clientY - mappingPanRef.current.startY;
                setMappingCanvasTf((prev) => ({
                    ...prev,
                    x: mappingPanRef.current.originX + dx,
                    y: mappingPanRef.current.originY + dy,
                }));
            }
        };

        const handleMouseUp = () => {
            mappingPanRef.current.active = false;
            mappingBoxDragRef.current.active = false;
        };

        const handleKeyDown = (event) => {
            if (event.key === "Escape") {
                setShowMappingCanvas(false);
            }
        };

        window.addEventListener("mousemove", handleMouseMove);
        window.addEventListener("mouseup", handleMouseUp);
        window.addEventListener("keydown", handleKeyDown);
        return () => {
            window.removeEventListener("mousemove", handleMouseMove);
            window.removeEventListener("mouseup", handleMouseUp);
            window.removeEventListener("keydown", handleKeyDown);
        };
    }, [mappingCanvasTf.scale, showMappingCanvas]);

    useEffect(() => {
        if (!showMappingCanvas) {
            return;
        }

        const timer = window.requestAnimationFrame(() => {
            fitMappingCanvas();
        });

        return () => window.cancelAnimationFrame(timer);
    }, [fitMappingCanvas, mappingCanvasTargets.length, mappingSourceColumns.length, mappingTargetBoxWidth, showMappingCanvas]);

    useEffect(() => {
        let cancelled = false;

        async function loadMappingMetadata() {
            const activeConnectionId = Number(pipelineForm.connection_id || connectionId || 0);
            const sourceSchema = normalizedMappingConfig.source.schema || pipelineForm.schema_name || schemaName || "";
            const sourceObjectName = normalizedMappingConfig.source.object || "";
            const targetSchema = normalizedMappingConfig.target.schema || "";
            const targetObjectName = normalizedMappingConfig.target.object || "";

            if (!activeConnectionId || !pipelineForm.database_name || !sourceSchema || !sourceObjectName) {
                setMappingSourceDetails(null);
                setMappingTargetDetails(null);
                setMappingMetadataError("");
                return;
            }

            setMappingMetadataLoading(true);
            setMappingMetadataError("");

            try {
                const sourcePromise = fetchObjectDetails(
                    activeConnectionId,
                    pipelineForm.database_name,
                    sourceSchema,
                    sourceObjectName
                );
                const targetPromise = targetSchema && targetObjectName
                    ? fetchObjectDetails(
                        activeConnectionId,
                        pipelineForm.database_name,
                        targetSchema,
                        targetObjectName
                    ).catch((err) => {
                        if ((err.message || "").includes("Object not found")) {
                            return null;
                        }
                        throw err;
                    })
                    : Promise.resolve(null);

                const [sourceDetails, targetDetails] = await Promise.all([sourcePromise, targetPromise]);
                if (cancelled) {
                    return;
                }

                setMappingSourceDetails(sourceDetails || null);
                setMappingTargetDetails(targetDetails || null);
            } catch (err) {
                if (cancelled) {
                    return;
                }
                setMappingSourceDetails(null);
                setMappingTargetDetails(null);
                setMappingMetadataError(err.message || "Failed to load mapping metadata.");
            } finally {
                if (!cancelled) {
                    setMappingMetadataLoading(false);
                }
            }
        }

        loadMappingMetadata();

        return () => {
            cancelled = true;
        };
    }, [
        connectionId,
        normalizedMappingConfig.source.object,
        normalizedMappingConfig.source.schema,
        normalizedMappingConfig.target.object,
        normalizedMappingConfig.target.schema,
        pipelineForm.connection_id,
        pipelineForm.database_name,
        pipelineForm.schema_name,
        schemaName,
    ]);

    const handleGenerateMappingFromMetadata = useCallback(() => {
        if (!mappingSourceDetails?.columns?.length) {
            setError("Load a valid source object before generating a mapping.");
            return;
        }

        const mappedColumns = buildMappingColumnsFromMetadata(
            mappingSourceDetails.columns,
            mappingTargetDetails?.columns || []
        );

        setPipelineForm((prev) => ({
            ...prev,
            mapping_config: {
                ...syncMappingConfigWithHeader(prev.mapping_config, prev.schema_name, prev.source_object, prev.target_object),
                target: {
                    ...syncMappingConfigWithHeader(prev.mapping_config, prev.schema_name, prev.source_object, prev.target_object).target,
                    mode: mappingTargetDetails ? "existing" : "create_new",
                },
                columns: mappedColumns,
            },
        }));
        setMessage(
            mappingTargetDetails
                ? `Prepared ${mappedColumns.length} target node(s) from the current source and target metadata.`
                : `Prepared ${mappedColumns.length} target node(s) from the source metadata.`
        );
        setError("");
    }, [mappingSourceDetails, mappingTargetDetails]);

    const commitCanvasTargets = useCallback((updater) => {
        setPipelineForm((prev) => {
            const nextMapping = syncMappingConfigWithHeader(prev.mapping_config, prev.schema_name, prev.source_object, prev.target_object);
            const baseColumns = buildCanvasTargetColumns(nextMapping.columns, mappingTargetDetails);
            const nextColumns = updater(baseColumns.map((column) => normalizeMappingColumn(column)));

            return {
                ...prev,
                mapping_config: {
                    ...nextMapping,
                    columns: nextColumns.map((column) => normalizeMappingColumn(column)),
                },
            };
        });
    }, [mappingTargetDetails]);

    const handleAiSuggestCanvasMapping = useCallback(async () => {
        if (!mappingSourceColumns.length) {
            setError("Load source columns before asking AI to map them.");
            return;
        }
        if (!mappingCanvasTargets.length) {
            setError("Create or load target nodes on the canvas before asking AI to map them.");
            return;
        }

        try {
            setAiMappingLoading(true);
            setError("");
            setMessage("");

            const result = await aiSuggestPipelineMapping({
                connection_id: Number(pipelineForm.connection_id || connectionId || 0),
                database_name: pipelineForm.database_name || databaseName || "",
                source_schema: normalizedMappingConfig.source.schema || pipelineForm.schema_name || schemaName || "",
                source_object: normalizedMappingConfig.source.object || pipelineForm.source_object || selectedObject || "",
                target_schema: normalizedMappingConfig.target.schema || "",
                target_object: normalizedMappingConfig.target.object || "",
                mapping_config: {
                    ...normalizedMappingConfig,
                    columns: mappingCanvasTargets,
                },
            });

            commitCanvasTargets(() => (result.mapping_columns || []).map((column) => normalizeMappingColumn(column)));
            setMessage(
                result.used_llm
                    ? "AI mapped the current canvas based on source and target metadata. Review the links and save when ready."
                    : "Fallback mapping suggestions were applied because AI was unavailable. Review the links and save when ready."
            );
            setSelectedMappingSource("");
        } catch (err) {
            setError(err.message || "Failed to generate AI mapping suggestions.");
        } finally {
            setAiMappingLoading(false);
        }
    }, [
        commitCanvasTargets,
        connectionId,
        databaseName,
        mappingCanvasTargets,
        mappingSourceColumns.length,
        normalizedMappingConfig,
        pipelineForm.connection_id,
        pipelineForm.database_name,
        pipelineForm.schema_name,
        pipelineForm.source_object,
        schemaName,
        selectedObject,
    ]);

    const handleAddManualMappingColumn = useCallback(() => {
        setPipelineForm((prev) => {
            const nextMapping = syncMappingConfigWithHeader(prev.mapping_config, prev.schema_name, prev.source_object, prev.target_object);
            return {
                ...prev,
                mapping_config: {
                    ...nextMapping,
                    target: {
                        ...nextMapping.target,
                        mode: "create_new",
                    },
                    columns: [
                        ...nextMapping.columns,
                        normalizeMappingColumn({
                            source_column: "",
                            target_column: "",
                            data_type: "text",
                            is_nullable: true,
                            include: true,
                        }),
                    ],
                },
            };
        });
        setError("");
        setMessage("Added a blank target column to the mapping.");
    }, []);

    const handleMappingColumnChange = useCallback((index, field, value) => {
        commitCanvasTargets((columns) => columns.map((column, columnIndex) => {
                if (columnIndex !== index) {
                    return column;
                }
                return normalizeMappingColumn({
                    ...column,
                    [field]: value,
                });
            }));
    }, [commitCanvasTargets]);

    const handleRemoveMappingColumn = useCallback((index) => {
        commitCanvasTargets((columns) => columns.filter((_, columnIndex) => columnIndex !== index));
    }, [commitCanvasTargets]);

    const handleCanvasTargetMap = useCallback((targetIndex) => {
        if (!selectedMappingSource) {
            setError("Select a source column first, then choose a target column on the canvas.");
            return;
        }

        handleMappingColumnChange(targetIndex, "source_column", selectedMappingSource);
        setSelectedMappingSource("");
        setError("");
        setMessage(`Mapped ${selectedMappingSource} to ${mappingCanvasTargets[targetIndex]?.target_column || "target column"}.`);
    }, [handleMappingColumnChange, mappingCanvasTargets, selectedMappingSource]);

    const handleClearTargetMapping = useCallback((targetIndex) => {
        handleMappingColumnChange(targetIndex, "source_column", "");
        setError("");
        setMessage("Removed the source-to-target link.");
    }, [handleMappingColumnChange]);

    const updateTargetSelection = useCallback(({ schema, object, mode, clearColumns = false }) => {
        setPipelineForm((prev) => {
            const nextMapping = syncMappingConfigWithHeader(prev.mapping_config, prev.schema_name, prev.source_object, prev.target_object);
            const nextSchema = normalizeIdentifierValue(schema ?? nextMapping.target.schema);
            const nextObject = normalizeIdentifierValue(object ?? nextMapping.target.object);
            const nextMode = mode || nextMapping.target.mode;

            return {
                ...prev,
                target_object: formatQualifiedObjectName(nextSchema, nextObject),
                mapping_config: {
                    ...nextMapping,
                    target: {
                        ...nextMapping.target,
                        schema: nextSchema,
                        object: nextObject,
                        mode: nextMode,
                    },
                    columns: clearColumns ? [] : nextMapping.columns,
                },
            };
        });
    }, []);

    const handleTargetSchemaSelect = useCallback((nextSchema) => {
        const isExistingTargetMode = normalizedMappingConfig.target.mode === "existing";
        updateTargetSelection({
            schema: nextSchema,
            object: isExistingTargetMode ? "" : selectedTargetReference.object,
            mode: normalizedMappingConfig.target.mode,
            clearColumns: isExistingTargetMode,
        });
        setError("");
        setMessage(nextSchema ? `Target schema set to ${nextSchema}.` : "Choose a target schema to anchor AI generation.");
    }, [normalizedMappingConfig.target.mode, selectedTargetReference.object, updateTargetSelection]);

    const handleExistingTargetSelect = useCallback((nextObject) => {
        updateTargetSelection({
            schema: selectedTargetReference.schema,
            object: nextObject,
            mode: "existing",
            clearColumns: true,
        });
        setError("");
        setMessage(
            nextObject
                ? `Target anchored to ${formatQualifiedObjectName(selectedTargetReference.schema, nextObject)}.`
                : "Choose a target object to anchor AI generation."
        );
    }, [selectedTargetReference.schema, updateTargetSelection]);

    const handleCreateTargetNameChange = useCallback((nextObject) => {
        updateTargetSelection({
            schema: selectedTargetReference.schema,
            object: nextObject,
            mode: "create_new",
        });
        setError("");
    }, [selectedTargetReference.schema, updateTargetSelection]);

    const handleMappingTargetModeChange = useCallback((mode) => {
        setPipelineForm((prev) => {
            const nextMapping = syncMappingConfigWithHeader(prev.mapping_config, prev.schema_name, prev.source_object, prev.target_object);
            return {
                ...prev,
                mapping_config: {
                    ...nextMapping,
                    target: {
                        ...nextMapping.target,
                        mode,
                    },
                },
            };
        });
    }, []);

    const handleApplyStepTemplate = useCallback(() => {
        const quickTemplateContext = resolveQuickTemplateContext({
            pipelineForm,
            schemaName,
            selectedObject,
        });

        const template = buildQuickStepTemplate(
            selectedStepTemplateId,
            quickTemplateContext.sourceSchema,
            quickTemplateContext.targetSchema,
            quickTemplateContext.sourceObject,
            quickTemplateContext.targetObject
        );

        setStepForm((prev) => ({
            ...prev,
            step_name: template.stepName,
            sql_text: template.sqlText,
        }));
        const explicitTarget = parseSchemaObject(pipelineForm?.target_object || "");
        const targetUsesSourceSchema = !explicitTarget.schema
            && quickTemplateContext.targetSchema
            && quickTemplateContext.targetSchema.toLowerCase() === quickTemplateContext.sourceSchema.toLowerCase();

        setMessage(
            targetUsesSourceSchema
                ? `Loaded ${selectedStepTemplateMeta.label} template. Target Object is not schema-qualified, so the SQL uses ${quickTemplateContext.targetSchema}. Enter a qualified target such as CORE.customer_s if you want the template to create it in another schema.`
                : `Loaded ${selectedStepTemplateMeta.label} template into the step editor.`
        );
        setError("");
    }, [
        selectedStepTemplateId,
        pipelineForm.schema_name,
        pipelineForm.source_object,
        pipelineForm.target_object,
        schemaName,
        selectedObject,
        selectedStepTemplateMeta,
    ]);

    const handleAgentGenerate = useCallback(async () => {
        if (!selectedPipelineId) {
            setAgentError("Select or create a pipeline first.");
            return;
        }
        if (hasUnsavedPipelineDefinition) {
            setAgentError("Save the pipeline header and source-to-target mapping before using AI Generate Steps.");
            return;
        }
        if (!agentRequirement.trim()) {
            setAgentError("Please describe what this pipeline should do.");
            return;
        }
        if (!selectedTargetReference.schema || !selectedTargetReference.object) {
            setAgentError("Select a target schema and target object in Source to Target Mapping before using AI Generate Steps.");
            return;
        }

        setAgentGenerating(true);
        setAgentError("");
        setAgentResult(null);
        setAgentAddedSteps(new Set());
        setAgentExpandedSteps({});

        try {
            let result = await agenticGeneratePipelineSteps(
                selectedPipelineId,
                agentRequirement.trim(),
                [],
                { allowCreateTarget: false }
            );

            if (result?.confirmation_required) {
                const approved = window.confirm(
                    result.confirmation_message ||
                    "This request needs approval to generate CREATE SCHEMA / CREATE TABLE steps for a new target. Do you want to continue?"
                );

                if (!approved) {
                    setAgentError("Agentic generation cancelled. New target creation was not approved.");
                    return;
                }

                result = await agenticGeneratePipelineSteps(
                    selectedPipelineId,
                    agentRequirement.trim(),
                    [],
                    { allowCreateTarget: true }
                );
            }

            setAgentResult(result);
        } catch (err) {
            setAgentError(err.message || "Agentic generation failed");
        } finally {
            setAgentGenerating(false);
        }
    }, [selectedPipelineId, hasUnsavedPipelineDefinition, agentRequirement, selectedTargetReference.object, selectedTargetReference.schema]);

    const handleAgentAddStep = useCallback(async (step, idx) => {
        if (!selectedPipelineId) return;
        try {
            const updated = await addPipelineStep(selectedPipelineId, {
                step_name: step.step_name,
                sql_text: step.sql_text,
                step_type: "sql",
                is_active: true,
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
                    is_active: true,
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
                                        <label>Target Object (schema.object allowed)</label>
                                        <input
                                            type="text"
                                            value={pipelineForm.target_object}
                                            onChange={(e) =>
                                                setPipelineForm((prev) => ({ ...prev, target_object: e.target.value }))
                                            }
                                            placeholder="e.g. CORE.customers_clean"
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

                            <div className="pipeline-main-card pipeline-mapping-card">
                                <div className="pipeline-card-header">
                                    <div>
                                        <h3>Source to Target Mapping</h3>
                                        <p className="pipeline-card-subtitle">
                                            Define the target shape before generating steps. Saved mappings are passed back into Agentic AI so create-target requests stay aligned to the target you approved.
                                        </p>
                                    </div>
                                </div>

                                <div className="pipeline-run-summary-grid pipeline-mapping-summary-grid">
                                    <div className="pipeline-summary-box">
                                        <div className="pipeline-summary-label">Source</div>
                                        <div className="pipeline-summary-value">
                                            {normalizedMappingConfig.source.schema || pipelineForm.schema_name || "-"}
                                            {normalizedMappingConfig.source.object ? `.${normalizedMappingConfig.source.object}` : ""}
                                        </div>
                                    </div>

                                    <div className="pipeline-summary-box">
                                        <div className="pipeline-summary-label">Target</div>
                                        <div className="pipeline-summary-value">
                                            {normalizedMappingConfig.target.schema || "-"}
                                            {normalizedMappingConfig.target.object ? `.${normalizedMappingConfig.target.object}` : ""}
                                        </div>
                                    </div>

                                    <div className="pipeline-summary-box">
                                        <div className="pipeline-summary-label">Mapped Columns</div>
                                        <div className="pipeline-summary-value">
                                            {normalizedMappingConfig.columns.filter((column) => column.include && column.target_column).length}
                                        </div>
                                    </div>

                                    <div className="pipeline-summary-box">
                                        <div className="pipeline-summary-label">AI Readiness</div>
                                        <div className="pipeline-summary-value">
                                            {selectedPipelineId
                                                ? hasUnsavedPipelineDefinition
                                                    ? "Save Required"
                                                    : "Saved"
                                                : "Create Pipeline"}
                                        </div>
                                    </div>
                                </div>

                                <div className="pipeline-mapping-mode-row">
                                    <button
                                        type="button"
                                        className={`pipeline-chip-btn ${normalizedMappingConfig.target.mode === "existing" ? "active" : ""}`}
                                        onClick={() => handleMappingTargetModeChange("existing")}
                                    >
                                        Use Existing Target
                                    </button>
                                    <button
                                        type="button"
                                        className={`pipeline-chip-btn ${normalizedMappingConfig.target.mode === "create_new" ? "active" : ""}`}
                                        onClick={() => handleMappingTargetModeChange("create_new")}
                                    >
                                        Create Target From Mapping
                                    </button>
                                    <span className="pipeline-mini-muted">
                                        {mappingTargetDetails
                                            ? "The target object already exists. You can still switch to a new target draft if you want to redesign it first."
                                            : normalizedMappingConfig.target.object
                                                ? "The target object does not exist yet. Save this mapping, then Agentic AI will ask for permission before generating CREATE TABLE steps."
                                                : "Set a target object in the pipeline header to anchor the mapping."}
                                    </span>
                                </div>

                                <div className="pipeline-mapping-target-picker-grid">
                                    <div className="form-field">
                                        <label>Target Schema</label>
                                        <select
                                            value={selectedTargetReference.schema}
                                            onChange={(e) => handleTargetSchemaSelect(e.target.value)}
                                        >
                                            <option value="">Select target schema</option>
                                            {targetSchemaOptions.map((schemaOption) => (
                                                <option key={schemaOption} value={schemaOption}>{schemaOption}</option>
                                            ))}
                                        </select>
                                    </div>

                                    {normalizedMappingConfig.target.mode === "existing" ? (
                                        <div className="form-field">
                                            <label>Existing Target Object</label>
                                            <select
                                                value={selectedTargetReference.object}
                                                onChange={(e) => handleExistingTargetSelect(e.target.value)}
                                                disabled={!selectedTargetReference.schema || targetPickerLoading}
                                            >
                                                <option value="">
                                                    {!selectedTargetReference.schema
                                                        ? "Choose target schema first"
                                                        : targetPickerLoading
                                                            ? "Loading target objects..."
                                                            : "Select target object"}
                                                </option>
                                                {targetObjectOptions.map((objectOption) => (
                                                    <option key={`${objectOption.schema || selectedTargetReference.schema}.${objectOption.name}`} value={objectOption.name}>
                                                        {objectOption.name}{objectOption.type ? ` (${objectOption.type})` : ""}
                                                    </option>
                                                ))}
                                            </select>
                                        </div>
                                    ) : (
                                        <div className="form-field">
                                            <label>New Target Object</label>
                                            <input
                                                type="text"
                                                value={selectedTargetReference.object}
                                                onChange={(e) => handleCreateTargetNameChange(e.target.value)}
                                                placeholder={selectedTargetReference.schema ? "e.g. customer_s" : "Choose target schema first"}
                                                disabled={!selectedTargetReference.schema}
                                            />
                                        </div>
                                    )}
                                </div>

                                {targetPickerError ? (
                                    <div className="pipeline-target-picker-note error">{targetPickerError}</div>
                                ) : (
                                    <div className="pipeline-target-picker-note">
                                        {selectedTargetReference.schema && selectedTargetReference.object
                                            ? `AI will anchor step generation to ${formatQualifiedObjectName(selectedTargetReference.schema, selectedTargetReference.object)}.`
                                            : "Select a target schema and object here so AI uses the intended destination instead of inferring from the source."}
                                    </div>
                                )}

                                {(mappingMetadataError || starterMetadataStatus) && (
                                    <div className="pipeline-starter-note">
                                        {mappingMetadataError || starterMetadataStatus}
                                    </div>
                                )}

                                <div className="pipeline-mapping-preview-card">
                                    <div className="pipeline-mapping-preview-header">
                                        <div>
                                            <div className="pipeline-mapping-preview-title">Mapping Canvas</div>
                                            <div className="pipeline-mini-muted">
                                                Open a movable canvas with two table boxes, then draw column links visually.
                                            </div>
                                        </div>

                                        <button
                                            className="pipeline-primary-btn"
                                            type="button"
                                            onClick={() => setShowMappingCanvas(true)}
                                        >
                                            Open Mapping Canvas
                                        </button>
                                    </div>

                                    <div className="pipeline-mapping-preview-stage">
                                        <div className="pipeline-mapping-preview-table source">
                                            <div className="pipeline-mapping-preview-table-header">
                                                {normalizedMappingConfig.source.schema || pipelineForm.schema_name || "src"}
                                                {normalizedMappingConfig.source.object ? `.${normalizedMappingConfig.source.object}` : ".source"}
                                            </div>
                                            <div className="pipeline-mapping-preview-col-list">
                                                {mappingSourceColumns.slice(0, 4).map((column) => (
                                                    <span key={column.column_name} className="pipeline-mapping-preview-col-chip">
                                                        {column.column_name}
                                                    </span>
                                                ))}
                                                {mappingSourceColumns.length > 4 ? (
                                                    <span className="pipeline-mapping-preview-col-chip muted">+{mappingSourceColumns.length - 4} more</span>
                                                ) : null}
                                            </div>
                                        </div>

                                        <div className="pipeline-mapping-preview-links">
                                            <span>{mappingConnections.length} link{mappingConnections.length === 1 ? "" : "s"}</span>
                                            <span className="pipeline-mini-muted">drag boxes • zoom • connect columns</span>
                                        </div>

                                        <div className="pipeline-mapping-preview-table target">
                                            <div className="pipeline-mapping-preview-table-header">
                                                {normalizedMappingConfig.target.schema || "target"}
                                                {normalizedMappingConfig.target.object ? `.${normalizedMappingConfig.target.object}` : ".table"}
                                            </div>
                                            <div className="pipeline-mapping-preview-col-list">
                                                {mappingCanvasTargets.slice(0, 4).map((column, index) => (
                                                    <span key={`${column.target_column || "target"}-${index}`} className="pipeline-mapping-preview-col-chip">
                                                        {column.target_column || "target_column"}
                                                    </span>
                                                ))}
                                                {mappingCanvasTargets.length > 4 ? (
                                                    <span className="pipeline-mapping-preview-col-chip muted">+{mappingCanvasTargets.length - 4} more</span>
                                                ) : null}
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                {!mappingSourceColumns.length && !mappingCanvasTargets.length ? (
                                    <div className="pipeline-empty">
                                        Load source metadata first, then open the mapping canvas to connect source columns to target columns visually.
                                    </div>
                                ) : null}
                            </div>

                            {showMappingCanvas && (
                                <div className="pipeline-mapping-modal-overlay" role="dialog" aria-modal="true" aria-label="Pipeline mapping canvas">
                                    <div className="pipeline-mapping-modal-card">
                                        <div className="pipeline-mapping-modal-header">
                                            <div>
                                                <h3>Column Mapping Canvas</h3>
                                                <p className="pipeline-card-subtitle">
                                                    Drag the source and target boxes, scroll to zoom, drag the background to pan, and connect source columns to target columns.
                                                </p>
                                            </div>

                                            <div className="pipeline-actions-row">
                                                <button className="pipeline-secondary-btn" type="button" onClick={fitMappingCanvas}>Fit View</button>
                                                <button className="pipeline-secondary-btn" type="button" onClick={() => zoomMappingCanvas(1.12)}>Zoom In</button>
                                                <button className="pipeline-secondary-btn" type="button" onClick={() => zoomMappingCanvas(1 / 1.12)}>Zoom Out</button>
                                                <button className="pipeline-danger-btn" type="button" onClick={() => setShowMappingCanvas(false)}>Close</button>
                                            </div>
                                        </div>

                                        <div className="pipeline-mapping-canvas-toolbar">
                                            <button
                                                className="pipeline-primary-btn"
                                                type="button"
                                                disabled={aiMappingLoading || !mappingSourceColumns.length || !mappingCanvasTargets.length}
                                                onClick={handleAiSuggestCanvasMapping}
                                            >
                                                {aiMappingLoading ? "AI Mapping..." : "AI Map Columns"}
                                            </button>
                                            <button
                                                className="pipeline-secondary-btn"
                                                type="button"
                                                disabled={mappingMetadataLoading || !mappingSourceDetails?.columns?.length}
                                                onClick={handleGenerateMappingFromMetadata}
                                            >
                                                {mappingMetadataLoading ? "Loading metadata..." : "Prepare Target Nodes"}
                                            </button>
                                            <button className="pipeline-secondary-btn" type="button" onClick={handleAddManualMappingColumn}>Add Target Column</button>
                                            {selectedMappingSource ? (
                                                <button className="pipeline-secondary-btn" type="button" onClick={() => setSelectedMappingSource("")}>Clear Selected Source</button>
                                            ) : null}
                                        </div>

                                        <div
                                            ref={mappingViewportRef}
                                            className="pipeline-mapping-modal-viewport"
                                            onMouseDown={handleMappingViewportMouseDown}
                                            onWheel={handleMappingViewportWheel}
                                        >
                                            <div
                                                className="pipeline-mapping-modal-stage"
                                                style={{
                                                    width: `${MAPPING_MODAL_STAGE_WIDTH}px`,
                                                    height: `${mappingModalStageHeight}px`,
                                                    transform: `translate(${mappingCanvasTf.x}px, ${mappingCanvasTf.y}px) scale(${mappingCanvasTf.scale})`,
                                                }}
                                            >
                                                <svg className="pipeline-mapping-modal-svg" width={MAPPING_MODAL_STAGE_WIDTH} height={mappingModalStageHeight}>
                                                    {mappingModalConnections.map((connection) => (
                                                        <g key={`${connection.sourceColumn}-${connection.targetColumn}-${connection.targetIndex}`}>
                                                            <path d={connection.path} className="pipeline-mapping-svg-path" />
                                                            <circle cx={connection.startX} cy={connection.startY} r="4" className="pipeline-mapping-svg-dot" />
                                                            <circle cx={connection.endX} cy={connection.endY} r="4" className="pipeline-mapping-svg-dot" />
                                                        </g>
                                                    ))}
                                                </svg>

                                                <div
                                                    className="pipeline-mapper-box source"
                                                    style={{ left: `${mappingBoxPositions.source.x}px`, top: `${mappingBoxPositions.source.y}px`, width: `${MAPPING_MODAL_BOX_WIDTH}px` }}
                                                >
                                                    <button className="pipeline-mapper-box-handle" type="button" onMouseDown={(event) => beginMappingBoxDrag("source", event)}>
                                                        Drag Source Box
                                                    </button>
                                                    <div className="pipeline-mapper-box-title">
                                                        {normalizedMappingConfig.source.schema || pipelineForm.schema_name || "src"}
                                                        {normalizedMappingConfig.source.object ? `.${normalizedMappingConfig.source.object}` : ".source"}
                                                    </div>
                                                    <div className="pipeline-mapper-col-list">
                                                        {mappingSourceColumns.length ? mappingSourceColumns.map((column) => {
                                                            const sourceKey = String(column.column_name || "");
                                                            const isSelected = selectedMappingSource.toLowerCase() === sourceKey.toLowerCase();
                                                            const usageCount = mappingSourceUsage.get(sourceKey.toLowerCase()) || 0;
                                                            return (
                                                                <button
                                                                    key={sourceKey}
                                                                    type="button"
                                                                    className={`pipeline-mapper-col-row ${isSelected ? "active" : ""}`}
                                                                    onClick={() => setSelectedMappingSource(sourceKey)}
                                                                >
                                                                    <span>{column.column_name}</span>
                                                                    <span className="pipeline-mapper-col-meta">{usageCount ? `${usageCount} link` : column.data_type}</span>
                                                                </button>
                                                            );
                                                        }) : (
                                                            <div className="pipeline-empty pipeline-mapper-empty">No source columns loaded.</div>
                                                        )}
                                                    </div>
                                                </div>

                                                <div
                                                    className="pipeline-mapper-box target"
                                                    style={{ left: `${mappingBoxPositions.target.x}px`, top: `${mappingBoxPositions.target.y}px`, width: `${mappingTargetBoxWidth}px` }}
                                                >
                                                    <button className="pipeline-mapper-box-handle" type="button" onMouseDown={(event) => beginMappingBoxDrag("target", event)}>
                                                        Drag Target Box
                                                    </button>
                                                    <div className="pipeline-mapper-box-title">
                                                        {normalizedMappingConfig.target.schema || "target"}
                                                        {normalizedMappingConfig.target.object ? `.${normalizedMappingConfig.target.object}` : ".table"}
                                                    </div>
                                                    <div className="pipeline-mapper-col-list">
                                                        {mappingCanvasTargets.length ? mappingCanvasTargets.map((column, index) => (
                                                            <div key={`mapping-target-${index}`} className={`pipeline-mapper-col-row target ${column.source_column ? "linked" : ""}`}>
                                                                <button
                                                                    type="button"
                                                                    className={`pipeline-mapper-link-btn ${selectedMappingSource ? "ready" : ""}`}
                                                                    onClick={() => handleCanvasTargetMap(index)}
                                                                >
                                                                    {selectedMappingSource ? "Connect" : (column.source_column || "Select source")}
                                                                </button>
                                                                <input
                                                                    type="text"
                                                                    className="pipeline-mapper-target-input"
                                                                    value={column.target_column}
                                                                    onChange={(e) => handleMappingColumnChange(index, "target_column", e.target.value)}
                                                                    placeholder="Target column"
                                                                    size={getMappingInputWidthCh(column.target_column, 14, 30)}
                                                                />
                                                                <input
                                                                    type="text"
                                                                    className="pipeline-mapper-type-input"
                                                                    value={column.data_type}
                                                                    onChange={(e) => handleMappingColumnChange(index, "data_type", e.target.value)}
                                                                    placeholder="Type"
                                                                    size={getMappingInputWidthCh(column.data_type, 10, 18)}
                                                                />
                                                                <div className="pipeline-mapper-col-actions">
                                                                    <label className="pipeline-mapping-toggle-chip">
                                                                        <input
                                                                            type="checkbox"
                                                                            checked={column.is_nullable !== false}
                                                                            onChange={(e) => handleMappingColumnChange(index, "is_nullable", e.target.checked)}
                                                                        />
                                                                        <span>Nullable</span>
                                                                    </label>
                                                                    <label className="pipeline-mapping-toggle-chip">
                                                                        <input
                                                                            type="checkbox"
                                                                            checked={column.include !== false}
                                                                            onChange={(e) => handleMappingColumnChange(index, "include", e.target.checked)}
                                                                        />
                                                                        <span>Include</span>
                                                                    </label>
                                                                    {column.source_column ? (
                                                                        <button type="button" className="pipeline-secondary-btn" onClick={() => handleClearTargetMapping(index)}>Clear</button>
                                                                    ) : null}
                                                                    <button type="button" className="pipeline-danger-btn pipeline-mapping-remove-btn" onClick={() => handleRemoveMappingColumn(index)}>Remove</button>
                                                                </div>
                                                            </div>
                                                        )) : (
                                                            <div className="pipeline-empty pipeline-mapper-empty">No target columns available yet. Prepare nodes or add a target column first.</div>
                                                        )}
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            )}

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
                                                        {agentResult.target_resolution?.target_label && (
                                                            <span className="agent-plan-summary">
                                                                Target: {agentResult.target_resolution.target_label}
                                                                {agentResult.target_resolution.target_exists ? " (existing)" : " (new target approved)"}
                                                            </span>
                                                        )}
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
                                    <div className="pipeline-step-template-row">
                                        <div className="form-field">
                                            <label>Quick Step Template</label>
                                            <select
                                                value={selectedStepTemplateId}
                                                onChange={(e) => setSelectedStepTemplateId(e.target.value)}
                                            >
                                                {STEP_TEMPLATE_OPTIONS.map((item) => (
                                                    <option key={item.id} value={item.id}>
                                                        {item.label}
                                                    </option>
                                                ))}
                                            </select>
                                        </div>

                                        <button
                                            className="pipeline-secondary-btn"
                                            type="button"
                                            onClick={handleApplyStepTemplate}
                                        >
                                            Use Template
                                        </button>
                                    </div>

                                    <div className="pipeline-mini-muted">
                                        {selectedStepTemplateMeta.summary}
                                    </div>

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