import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
    fetchLineage,
    fetchDatabases,
    fetchSchemas,
    fetchObjects,
    saveConnection,
    generateAiSql,
    resolveObjects,
    executeQuery,
    fetchObjectDetails,
    fetchObjectSampleData,
    getAITransformationSuggestions,
    saveTransformationRecipe,
    fetchTransformationRecipes,
    deleteTransformationRecipe,
    fetchPromptHistory,
    generateDataQualitySql,
} from "../api/schemaApi";
import PipelineBuilder from "../components/PipelineBuilder";
import { useAppContext } from "../context/AppContext";

function LineageList({ title, items, emptyText, onNavigate }) {
    return (
        <div className="lineage-section-card">
            <div className="lineage-section-header">
                <h3>{title}</h3>
            </div>

            {items.length ? (
                <div className="lineage-list">
                    {items.map((item, index) => (
                        <button
                            type="button"
                            key={`${item.schema_name}-${item.object_name}-${item.relation_kind}-${index}`}
                            className="lineage-item lineage-item-button"
                            onClick={() => onNavigate?.(item)}
                        >
                            <div className="lineage-item-main">
                                <div className="lineage-object-name">
                                    {item.schema_name}.{item.object_name}
                                </div>
                                <div className="lineage-object-type">
                                    {item.object_type || "Object"}
                                </div>
                            </div>

                            <div className="lineage-relation-chip">
                                {item.relation_kind || "related"}
                            </div>
                        </button>
                    ))}
                </div>
            ) : (
                <div className="object-details-empty">{emptyText}</div>
            )}
        </div>
    );
}

function TabButton({ active, children, onClick }) {
    return (
        <button
            type="button"
            className={`etl-tab-btn ${active ? "etl-tab-btn-active" : ""}`}
            onClick={onClick}
        >
            {children}
        </button>
    );
}

function StatCard({ label, value, subtle }) {
    return (
        <div className="lineage-overview-box">
            <div className="lineage-kpi-label">{label}</div>
            <div className={`lineage-kpi-value ${subtle ? "lineage-kpi-value-subtle" : ""}`}>
                {value}
            </div>
        </div>
    );
}

function SectionCard({ title, subtitle, children, actions = null }) {
    return (
        <div className="transformations-card secondary-surface">
            <div className={`lineage-section-header ${actions ? "lineage-section-header-with-actions" : ""}`}>
                <div className="lineage-section-copy">
                    <h3>{title}</h3>
                    {subtitle ? (
                        <p className="section-caption" style={{ marginTop: "6px" }}>
                            {subtitle}
                        </p>
                    ) : null}
                </div>
                {actions}
            </div>
            {children}
        </div>
    );
}

function makeRelationKey(schemaName, objectName) {
    if (!schemaName || !objectName) return "";
    return `${String(schemaName).trim()}.${String(objectName).trim()}`;
}

function truncateLabel(value, maxLength = 34) {
    const text = String(value || "");
    if (text.length <= maxLength) return text;
    return `${text.slice(0, maxLength - 1)}...`;
}

function buildLineageGraphLayout(nodes, edges, rootKey) {
    if (!nodes.length) {
        return {
            nodes: [],
            edges: [],
            width: 920,
            height: 260,
        };
    }

    const outgoing = new Map();
    const incoming = new Map();

    const link = (map, from, to) => {
        if (!map.has(from)) {
            map.set(from, new Set());
        }
        map.get(from).add(to);
    };

    edges.forEach((edge) => {
        if (!edge?.sourceKey || !edge?.targetKey) return;
        link(outgoing, edge.sourceKey, edge.targetKey);
        link(incoming, edge.targetKey, edge.sourceKey);
    });

    const bfsDistances = (adjacency) => {
        const distances = new Map();
        const queue = [{ key: rootKey, dist: 0 }];
        distances.set(rootKey, 0);

        while (queue.length) {
            const current = queue.shift();
            const neighbors = adjacency.get(current.key);
            if (!neighbors) continue;

            neighbors.forEach((neighborKey) => {
                if (distances.has(neighborKey)) return;
                const nextDist = current.dist + 1;
                distances.set(neighborKey, nextDist);
                queue.push({ key: neighborKey, dist: nextDist });
            });
        }

        return distances;
    };

    const upstreamDistances = bfsDistances(incoming);
    const downstreamDistances = bfsDistances(outgoing);

    const withDepth = nodes.map((node) => {
        if (node.key === rootKey) {
            return { ...node, depth: 0 };
        }

        const upstreamDist = upstreamDistances.get(node.key);
        const downstreamDist = downstreamDistances.get(node.key);

        let depth = 0;
        if (Number.isFinite(upstreamDist) && Number.isFinite(downstreamDist)) {
            depth = downstreamDist <= upstreamDist ? downstreamDist : -upstreamDist;
        } else if (Number.isFinite(upstreamDist)) {
            depth = -upstreamDist;
        } else if (Number.isFinite(downstreamDist)) {
            depth = downstreamDist;
        }

        return { ...node, depth };
    });

    const byDepth = new Map();
    withDepth.forEach((node) => {
        const depth = Number.isFinite(node.depth) ? node.depth : 0;
        if (!byDepth.has(depth)) {
            byDepth.set(depth, []);
        }
        byDepth.get(depth).push(node);
    });

    const depthColumns = Array.from(byDepth.keys()).sort((a, b) => a - b);
    const NODE_WIDTH = 228;
    const NODE_HEIGHT = 66;
    const COLUMN_GAP = 84;
    const ROW_GAP = 24;
    const PADDING_X = 28;
    const PADDING_Y = 32;

    const maxRows = Math.max(
        1,
        ...depthColumns.map((depth) => byDepth.get(depth)?.length || 0)
    );

    const width =
        PADDING_X * 2 +
        depthColumns.length * NODE_WIDTH +
        Math.max(0, depthColumns.length - 1) * COLUMN_GAP;

    const height =
        PADDING_Y * 2 + maxRows * NODE_HEIGHT + Math.max(0, maxRows - 1) * ROW_GAP;

    const positionedNodes = [];
    depthColumns.forEach((depth, colIndex) => {
        const colNodes = [...(byDepth.get(depth) || [])].sort((a, b) =>
            `${a.schemaName}.${a.objectName}`.localeCompare(`${b.schemaName}.${b.objectName}`)
        );

        const contentHeight =
            colNodes.length * NODE_HEIGHT + Math.max(0, colNodes.length - 1) * ROW_GAP;
        const startY = Math.max(PADDING_Y, Math.round((height - contentHeight) / 2));

        colNodes.forEach((node, rowIndex) => {
            positionedNodes.push({
                ...node,
                width: NODE_WIDTH,
                height: NODE_HEIGHT,
                x: PADDING_X + colIndex * (NODE_WIDTH + COLUMN_GAP),
                y: startY + rowIndex * (NODE_HEIGHT + ROW_GAP),
            });
        });
    });

    const nodeByKey = new Map(positionedNodes.map((node) => [node.key, node]));

    const positionedEdges = edges
        .filter((edge) => nodeByKey.has(edge.sourceKey) && nodeByKey.has(edge.targetKey))
        .map((edge, index) => {
            const source = nodeByKey.get(edge.sourceKey);
            const target = nodeByKey.get(edge.targetKey);

            return {
                id: `${edge.sourceKey}->${edge.targetKey}:${index}`,
                ...edge,
                source,
                target,
            };
        });

    return {
        nodes: positionedNodes,
        edges: positionedEdges,
        width,
        height,
    };
}

function LineageGraph({ graphData, graphLoading, graphError, onNavigate }) {
    const svgRef = useRef(null);
    const viewportRef = useRef(null);
    const dragRef = useRef({ active: false, startX: 0, startY: 0, originX: 0, originY: 0 });
    const [tf, setTf] = useState({ x: 0, y: 0, scale: 1 });

    // Non-passive wheel listener for zoom centered on cursor
    useEffect(() => {
        const el = svgRef.current;
        if (!el) return;
        const onWheel = (e) => {
            e.preventDefault();
            const rect = el.getBoundingClientRect();
            const mx = e.clientX - rect.left;
            const my = e.clientY - rect.top;
            const factor = e.deltaY < 0 ? 1.13 : 1 / 1.13;
            setTf(prev => {
                const ns = Math.min(Math.max(prev.scale * factor, 0.12), 6);
                const cx = (mx - prev.x) / prev.scale;
                const cy = (my - prev.y) / prev.scale;
                return { x: mx - cx * ns, y: my - cy * ns, scale: ns };
            });
        };
        el.addEventListener('wheel', onWheel, { passive: false });
        return () => el.removeEventListener('wheel', onWheel);
    }, []);

    // Fit to viewport when graph data first arrives
    useEffect(() => {
        if (!graphData || !viewportRef.current) return;
        const { width, height } = graphData;
        const vw = viewportRef.current.clientWidth || 700;
        const vh = 440;
        const pad = 48;
        const scale = Math.min((vw - pad * 2) / width, (vh - pad * 2) / height, 1.4);
        setTf({
            x: (vw - width * scale) / 2,
            y: (vh - height * scale) / 2,
            scale,
        });
    }, [graphData]);

    const onMouseDown = (e) => {
        if (e.button !== 0) return;
        e.currentTarget.style.cursor = 'grabbing';
        dragRef.current = { active: true, startX: e.clientX, startY: e.clientY, originX: tf.x, originY: tf.y, wasDragging: false };
    };
    const onMouseMove = (e) => {
        if (!dragRef.current.active) return;
        const dx = e.clientX - dragRef.current.startX;
        const dy = e.clientY - dragRef.current.startY;
        if (dx * dx + dy * dy > 9) dragRef.current.wasDragging = true;
        setTf(prev => ({ ...prev, x: dragRef.current.originX + dx, y: dragRef.current.originY + dy }));
    };
    const onMouseUp = (e) => {
        dragRef.current.active = false;
        if (e.currentTarget) e.currentTarget.style.cursor = 'grab';
    };

    const zoomBy = (factor) => {
        setTf(prev => {
            const el = svgRef.current;
            const cx = el ? el.clientWidth / 2 : 350;
            const cy = el ? el.clientHeight / 2 : 220;
            const ns = Math.min(Math.max(prev.scale * factor, 0.12), 6);
            const canvasX = (cx - prev.x) / prev.scale;
            const canvasY = (cy - prev.y) / prev.scale;
            return { x: cx - canvasX * ns, y: cy - canvasY * ns, scale: ns };
        });
    };

    const fitView = useCallback(() => {
        if (!graphData || !viewportRef.current) return;
        const { width, height } = graphData;
        const vw = viewportRef.current.clientWidth || 700;
        const vh = 440;
        const pad = 48;
        const scale = Math.min((vw - pad * 2) / width, (vh - pad * 2) / height, 1.4);
        setTf({
            x: (vw - width * scale) / 2,
            y: (vh - height * scale) / 2,
            scale,
        });
    }, [graphData]);

    if (graphLoading) {
        return (
            <div className="lineage-section-card lineage-graph-card">
                <div className="lineage-graph-status">Building lineage map...</div>
            </div>
        );
    }

    if (graphError) {
        return (
            <div className="lineage-section-card lineage-graph-card">
                <div className="object-details-error">{graphError}</div>
            </div>
        );
    }

    if (!graphData?.nodes?.length) {
        return (
            <div className="lineage-section-card lineage-graph-card">
                <div className="object-details-empty">
                    No lineage graph could be built for this object.
                </div>
            </div>
        );
    }

    const { nodes, edges, truncated } = graphData;

    return (
        <div className="lineage-section-card lineage-graph-card">
            <div className="lineage-section-header">
                <h3>Lineage Graph</h3>
                <p className="section-caption" style={{ marginTop: "6px" }}>
                    Visual flow of upstream sources to downstream consumers. Scroll to zoom · drag to pan · click a node to navigate.
                </p>
                {truncated ? (
                    <p className="section-caption" style={{ marginTop: "4px" }}>
                        Graph was capped to keep rendering responsive.
                    </p>
                ) : null}
            </div>

            <div className="lineage-graph-viewport" ref={viewportRef}>
                {/* Floating zoom controls */}
                <div className="lineage-zoom-controls">
                    <button
                        type="button"
                        className="lineage-zoom-btn"
                        onClick={() => zoomBy(1.25)}
                        title="Zoom in"
                    >+</button>
                    <span className="lineage-zoom-label">{Math.round(tf.scale * 100)}%</span>
                    <button
                        type="button"
                        className="lineage-zoom-btn"
                        onClick={() => zoomBy(1 / 1.25)}
                        title="Zoom out"
                    >−</button>
                    <div className="lineage-zoom-divider" />
                    <button
                        type="button"
                        className="lineage-zoom-btn lineage-zoom-btn-fit"
                        onClick={fitView}
                        title="Fit to view"
                    >Fit</button>
                </div>

                <svg
                    ref={svgRef}
                    className="lineage-graph-svg"
                    width="100%"
                    height="440"
                    style={{ cursor: 'grab', display: 'block' }}
                    onMouseDown={onMouseDown}
                    onMouseMove={onMouseMove}
                    onMouseUp={onMouseUp}
                    onMouseLeave={onMouseUp}
                    role="img"
                    aria-label="Lineage graph"
                >
                    <defs>
                        <marker
                            id="lineage-arrow"
                            viewBox="0 0 10 10"
                            refX="8"
                            refY="5"
                            markerWidth="8"
                            markerHeight="8"
                            orient="auto-start-reverse"
                        >
                            <path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor" />
                        </marker>
                        {nodes.map((node, idx) => (
                            <clipPath key={`cp-${idx}`} id={`nc-${idx}`}>
                                <rect x="10" y="4" width={node.width - 20} height={node.height - 8} />
                            </clipPath>
                        ))}
                    </defs>

                    <g transform={`translate(${tf.x.toFixed(2)},${tf.y.toFixed(2)}) scale(${tf.scale.toFixed(4)})`}>
                        {edges.map((edge) => {
                            const sourceX = edge.source.x + edge.source.width;
                            const targetX = edge.target.x;
                            const sourceY = edge.source.y + edge.source.height / 2;
                            const targetY = edge.target.y + edge.target.height / 2;
                            const distanceX = Math.abs(targetX - sourceX);
                            const bend = Math.max(40, distanceX * 0.35);

                            const path = `M ${sourceX} ${sourceY} C ${sourceX + bend} ${sourceY}, ${targetX - bend} ${targetY}, ${targetX} ${targetY}`;
                            const midX = Math.round((sourceX + targetX) / 2);
                            const midY = Math.round((sourceY + targetY) / 2);

                            const labelText = truncateLabel(edge.relationKind || "related", 14);
                            const labelW = Math.min(labelText.length * 6.5 + 12, 110);
                            return (
                                <g key={edge.id} className="lineage-graph-edge-group">
                                    <path
                                        d={path}
                                        className="lineage-graph-edge"
                                        markerEnd="url(#lineage-arrow)"
                                    />
                                    <rect
                                        x={midX - labelW / 2}
                                        y={midY - 17}
                                        width={labelW}
                                        height={14}
                                        rx="3"
                                        fill="white"
                                        opacity="0.88"
                                    />
                                    <text x={midX} y={midY - 6} className="lineage-graph-edge-label">
                                        {labelText}
                                    </text>
                                </g>
                            );
                        })}

                        {nodes.map((node, idx) => (
                            <g
                                key={node.key}
                                className={`lineage-graph-node ${node.isRoot ? "lineage-graph-node-root" : ""}`}
                                transform={`translate(${node.x}, ${node.y})`}
                                onClick={() => {
                                    if (dragRef.current.wasDragging) return;
                                    onNavigate?.({
                                        schema_name: node.schemaName,
                                        object_name: node.objectName,
                                    });
                                }}
                            >
                                <rect
                                    className="lineage-graph-node-rect"
                                    width={node.width}
                                    height={node.height}
                                    rx="14"
                                    ry="14"
                                />
                                <g clipPath={`url(#nc-${idx})`}>
                                    <text className="lineage-graph-node-title" x="12" y="24">
                                        {`${node.schemaName}.${node.objectName}`}
                                    </text>
                                    <text className="lineage-graph-node-subtitle" x="12" y="42">
                                        {node.objectType || "OBJECT"}
                                    </text>
                                </g>
                            </g>
                        ))}
                    </g>
                </svg>
            </div>
        </div>
    );
}

function LineageTab({ lineage, graphData, graphLoading, graphError, onNavigate }) {
    const flowText = useMemo(() => {
        const upstreamNames = (lineage.upstream || []).map(
            (item) => `${item.schema_name}.${item.object_name}`
        );
        const downstreamNames = (lineage.downstream || []).map(
            (item) => `${item.schema_name}.${item.object_name}`
        );

        return {
            upstream: upstreamNames.join(", "),
            current: `${lineage.schema_name}.${lineage.object_name}`,
            downstream: downstreamNames.join(", "),
        };
    }, [lineage]);

    return (
        <>
            <div className="lineage-overview-card">
                <div className="lineage-overview-grid">
                    <StatCard label="Object" value={lineage.object_name} subtle />
                    <StatCard label="Type" value={lineage.object_type || "Object"} subtle />
                    <StatCard label="Upstream Count" value={lineage.upstream?.length || 0} />
                    <StatCard label="Downstream Count" value={lineage.downstream?.length || 0} />
                </div>
            </div>

            <div className="lineage-summary-banner">
                <div className="lineage-summary-title">Lineage Summary</div>
                <div className="lineage-summary-text">
                    Review upstream dependencies to understand source inputs and downstream
                    dependencies to understand where this object is consumed.
                </div>

                <div style={{ marginTop: "10px" }}>
                    <div><strong>Upstream:</strong> {flowText.upstream || "None"}</div>
                    <div><strong>Current:</strong> {flowText.current}</div>
                    <div><strong>Downstream:</strong> {flowText.downstream || "None"}</div>
                </div>
            </div>

            <LineageGraph
                graphData={graphData}
                graphLoading={graphLoading}
                graphError={graphError}
                onNavigate={onNavigate}
            />

            <div className="lineage-grid">
                <LineageList
                    title="Upstream Dependencies"
                    items={lineage.upstream || []}
                    emptyText="No upstream dependencies found."
                    onNavigate={onNavigate}
                />

                <LineageList
                    title="Downstream Dependencies"
                    items={lineage.downstream || []}
                    emptyText="No downstream dependencies found."
                    onNavigate={onNavigate}
                />
            </div>
        </>
    );
}

function PromptChip({ children, onClick }) {
    return (
        <button
            type="button"
            className="prompt-chip"
            onClick={onClick}
        >
            {children}
        </button>
    );
}

function SuggestionCard({ title, description, onApply, onAppend }) {
    return (
        <div className="ai-suggestion-card">
            <div className="ai-suggestion-card-top">
                <div className="ai-suggestion-title">{title}</div>
            </div>

            <div className="ai-suggestion-text">{description}</div>

            <div className="ai-suggestion-actions">
                <button
                    type="button"
                    className="secondary-btn"
                    onClick={onApply}
                >
                    Use This
                </button>

                <button
                    type="button"
                    className="secondary-btn"
                    onClick={onAppend}
                >
                    Append
                </button>
            </div>
        </div>
    );
}

function formatPreviewCell(value) {
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

function normalizeList(value) {
    return Array.isArray(value) ? value.filter(Boolean) : [];
}

function isPreviewableSql(sql) {
    const text = (sql || "").trim().toLowerCase();
    return text.startsWith("select") || text.startsWith("with");
}

function buildPromptTemplates(selectedObject, selectedJoinTables) {
    const joinText = selectedJoinTables.length
        ? ` Include joins with: ${selectedJoinTables.join(", ")}.`
        : "";

    return [
        {
            id: "src-to-core-dedup",
            title: "Source → Core (Dedup)",
            prompt: `Load data from ${selectedObject} (source/staging layer) into a core/clean table with deduplication. Use ROW_NUMBER() partitioned by the primary/natural key columns (from the metadata) ordered by the most recent timestamp or ID to keep only the latest record per key. TRIM all text columns, COALESCE nullable columns with sensible defaults based on their data types, and CAST columns to proper types. Generate a CREATE TABLE AS SELECT or INSERT INTO ... SELECT.${joinText}`,
        },
        {
            id: "staging-load",
            title: "Staging Load (INSERT)",
            prompt: `Generate an INSERT INTO ... SELECT statement to load data from ${selectedObject} into a staging table. Use all actual columns from ${selectedObject} metadata. Apply TRIM on text/varchar columns, handle NULLs with COALESCE where appropriate, and add a load_timestamp column with CURRENT_TIMESTAMP. Generate INSERT INTO, not just SELECT.${joinText}`,
        },
        {
            id: "clean-transform-view",
            title: "Clean Transform (VIEW)",
            prompt: `Generate a CREATE OR REPLACE VIEW that transforms ${selectedObject} into a clean, analytics-ready format. Use actual column names and data types from the metadata. Rename columns to business-friendly aliases (e.g., cust_id → customer_id), TRIM text fields, CAST date/numeric columns properly, filter out obviously invalid rows (e.g., NULL keys), and add computed columns where the data types suggest it (e.g., age from birthdate, full_name from first+last).${joinText}`,
        },
        {
            id: "join-enrichment",
            title: "Join & Enrich",
            prompt: `Generate a SELECT query that enriches ${selectedObject} by joining it with related tables. Use the foreign key relationships and column metadata to determine join conditions. For each join, use the actual column names from both tables. Select key columns from ${selectedObject} and add descriptive/lookup columns from joined tables. Use LEFT JOIN to preserve all source rows.${joinText}`,
        },
        {
            id: "aggregate-metrics",
            title: "Aggregate Metrics",
            prompt: `Generate an aggregation query on ${selectedObject} using actual columns from metadata. Group by a meaningful dimension column (e.g., category, status, date) and compute metrics like COUNT(*), SUM/AVG on numeric columns, MIN/MAX on date columns. Use the real column names and appropriate aggregate functions based on each column's data type. Include a HAVING clause if there's a natural filter.${joinText}`,
        },
        {
            id: "scd-type2",
            title: "SCD Type 2 Table",
            prompt: `Create a Slowly Changing Dimension Type 2 table based on ${selectedObject}. Use the actual primary key columns from the metadata as the natural key. Generate a CREATE TABLE with: a surrogate_key (SERIAL or BIGSERIAL), all columns from ${selectedObject} with their correct data types, plus valid_from (TIMESTAMP DEFAULT CURRENT_TIMESTAMP), valid_to (TIMESTAMP DEFAULT '9999-12-31'), and is_current (BOOLEAN DEFAULT TRUE). Then generate a separate MERGE/INSERT statement that detects changes and manages historical records.`,
        },
        {
            id: "data-quality",
            title: "Data Quality Checks",
            prompt: `Generate a comprehensive data quality SQL query for ${selectedObject}. Use the actual column names and data types from metadata. Include: NULL count per column, duplicate check on key columns using GROUP BY HAVING COUNT(*) > 1, out-of-range checks on numeric columns, empty-string checks on text columns, and a summary row count. Return results as a single UNION ALL query or a WITH (CTE) block.${joinText}`,
        },
        {
            id: "incremental-load",
            title: "Incremental Load",
            prompt: `Generate an incremental/delta load query for ${selectedObject}. Identify the best timestamp or auto-increment column from the metadata to use as a watermark. Generate an INSERT INTO ... SELECT that filters rows WHERE that column > :last_loaded_value (use a placeholder parameter). Apply TRIM and COALESCE on the selected columns based on their data types. Include a comment explaining which column is the watermark and how to parameterize it.${joinText}`,
        },
    ];
}

function TemplateCard({ item, onReusePrompt, onDelete }) {
    return (
        <div className="ai-suggestion-card">
            <div className="ai-suggestion-card-top">
                <div className="ai-suggestion-title">
                    {item.recipe_name || item.statement_type || "Saved Transformation"}
                </div>
            </div>
            <div className="ai-suggestion-text">
                <strong>Type:</strong> {item.statement_type || "unknown"}<br />
                <strong>Saved:</strong> {item.created_at || "-"}<br />
                <strong>Prompt:</strong> {item.user_prompt || "-"}
            </div>
            <div className="ai-suggestion-actions">
                <button className="secondary-btn" type="button" onClick={() => onReusePrompt(item)}>
                    Use Prompt
                </button>
                <button className="secondary-btn" type="button" onClick={() => onDelete(item)}>
                    Delete
                </button>
            </div>
        </div>
    );
}

function QualityCard({ item, onUseSql }) {
    return (
        <div className="ai-suggestion-card">
            <div className="ai-suggestion-card-top">
                <div className="ai-suggestion-title">{item.title}</div>
            </div>
            <div className="ai-suggestion-text">
                {item.description}
            </div>
            <div className="ai-suggestion-actions">
                <button className="secondary-btn" type="button" onClick={() => onUseSql(item.sql)}>
                    Use SQL
                </button>
            </div>
        </div>
    );
}

function ColumnSuggestionCard({ item, onAppendPrompt }) {
    return (
        <div className="ai-suggestion-card">
            <div className="ai-suggestion-card-top">
                <div className="ai-suggestion-title">{item.title}</div>
            </div>
            <div className="ai-suggestion-text">
                <strong>Category:</strong> {item.category || "general"}<br />
                <strong>Impact:</strong> {item.impact || "-"}<br />
                {item.reason || ""}
            </div>
            <div className="ai-suggestion-actions">
                <button className="secondary-btn" type="button" onClick={() => onAppendPrompt(item)}>
                    Add to Prompt
                </button>
            </div>
        </div>
    );
}

function PromptEditor({
    sqlPrompt,
    setSqlPrompt,
    onClear,
    onCopy,
    selectedObject,
}) {
    return (
        <div className="form-field" style={{ marginBottom: "16px" }}>
            <div className="prompt-editor-header">
                <label>What SQL do you want?</label>
                <div className="prompt-editor-actions">
                    <button
                        type="button"
                        className="icon-btn"
                        title="Copy prompt"
                        onClick={onCopy}
                    >
                        ⧉
                    </button>
                    <button
                        type="button"
                        className="icon-btn icon-btn-danger"
                        title="Clear prompt"
                        onClick={onClear}
                    >
                        ×
                    </button>
                </div>
            </div>

            <textarea
                className="app-textarea transformation-prompt"
                value={sqlPrompt}
                onChange={(e) => setSqlPrompt(e.target.value)}
                placeholder={`Generate a useful SQL statement using ${selectedObject} as the base table.`}
                rows={7}
                style={{ width: "100%" }}
            />
        </div>
    );
}

function TransformationsTab(props) {
    const {
        sqlPrompt,
        setSqlPrompt,
        lineageTableOptions,
        selectedJoinTables,
        toggleJoinTable,
        selectAllJoinTables,
        clearJoinTables,
        handleGenerateSql,
        handleResolveObjects,
        handleDismissDisambiguation,
        handleConfirmAndGenerate,
        resolvedObjects,
        resolving,
        showDisambiguation,
        handleClearPrompt,
        handleCopyPrompt,
        handlePreviewSql,
        handleSaveTransformation,
        handleGenerateDataQuality,
        sqlLoading,
        sqlError,
        sqlResult,
        previewLoading,
        previewResult,
        previewError,
        readyForActions,
        selectedObject,
        selectedSchema,
        selectedDatabase,
        savedTemplates,
        promptHistory,
        transformationSuggestions,
        dataQualityResult,
        dataQualityLoading,
        templatesLoading,
        onReuseTemplatePrompt,
        onDeleteTemplate,
        onUseHistoryPrompt,
        onAppendSuggestionPrompt,
        onUseQualitySql,
    } = props;

    const templates = useMemo(
        () => buildPromptTemplates(selectedObject, selectedJoinTables),
        [selectedObject, selectedJoinTables]
    );

    const joinCandidatesCount = lineageTableOptions.filter(
        (tableName) => tableName !== selectedObject
    ).length;

    const canPreview = !!sqlResult?.sql && isPreviewableSql(sqlResult.sql);

    return (
        <div className="transformations-stack">
            <SectionCard
                title="AI Transformation Suggestions"
                subtitle="Statement-aware templates and lineage-aware generation."
            >
                <div className="lineage-overview-grid transformations-stats-grid" style={{ marginBottom: "16px" }}>
                    <StatCard label="Database" value={selectedDatabase} subtle />
                    <StatCard label="Schema" value={selectedSchema} subtle />
                    <StatCard label="Base Object" value={selectedObject} subtle />
                    <StatCard label="Join Candidates" value={joinCandidatesCount} />
                </div>

                <div className="form-field" style={{ marginBottom: "16px" }}>
                    <label>Prompt templates</label>
                    <div className="prompt-chip-row">
                        {templates.map((item) => (
                            <PromptChip key={item.id} onClick={() => setSqlPrompt(item.prompt)}>
                                {item.title}
                            </PromptChip>
                        ))}
                    </div>
                </div>

                <div className="ai-suggestion-grid ai-suggestion-grid-stacked">
                    {templates.map((item) => (
                        <SuggestionCard
                            key={item.id}
                            title={item.title}
                            description={item.prompt}
                            onApply={() => setSqlPrompt(item.prompt)}
                            onAppend={() =>
                                setSqlPrompt((prev) =>
                                    prev?.trim() ? `${prev.trim()}\n\n${item.prompt}` : item.prompt
                                )
                            }
                        />
                    ))}
                </div>
            </SectionCard>

            <SectionCard
                title="Column-aware Suggestions"
                subtitle="Suggestions grounded in the current object columns and sample data."
            >
                {transformationSuggestions?.summary ? (
                    <div className="info-banner" style={{ marginBottom: "12px" }}>
                        {transformationSuggestions.summary}
                    </div>
                ) : null}

                {transformationSuggestions?.suggestions?.length ? (
                    <div className="ai-suggestion-grid ai-suggestion-grid-stacked">
                        {transformationSuggestions.suggestions.map((item, idx) => (
                            <ColumnSuggestionCard
                                key={`${item.title}-${idx}`}
                                item={item}
                                onAppendPrompt={onAppendSuggestionPrompt}
                            />
                        ))}
                    </div>
                ) : (
                    <div className="object-details-empty">No AI column-aware suggestions yet.</div>
                )}
            </SectionCard>

            <SectionCard
                title="Prompt Builder"
                subtitle="Refine your request and choose lineage-based join candidates."
                actions={
                    <div className="action-row transformations-mini-actions" style={{ gap: "8px", marginBottom: 0 }}>
                        <button type="button" className="secondary-btn" onClick={selectAllJoinTables} disabled={!lineageTableOptions.length}>
                            Select All
                        </button>
                        <button type="button" className="secondary-btn" onClick={clearJoinTables} disabled={!selectedJoinTables.length}>
                            Clear Joins
                        </button>
                    </div>
                }
            >
                <PromptEditor
                    sqlPrompt={sqlPrompt}
                    setSqlPrompt={setSqlPrompt}
                    onClear={handleClearPrompt}
                    onCopy={handleCopyPrompt}
                    selectedObject={selectedObject}
                />

                <div className="form-field" style={{ marginBottom: "16px" }}>
                    <label>Tables to include as join candidates</label>
                    <div className="join-candidate-wrap">
                        {lineageTableOptions.length ? (
                            lineageTableOptions.map((tableName) => (
                                <label key={tableName} className="join-candidate-chip">
                                    <input
                                        type="checkbox"
                                        checked={selectedJoinTables.includes(tableName)}
                                        onChange={() => toggleJoinTable(tableName)}
                                        disabled={tableName === selectedObject}
                                    />
                                    <span>{tableName}</span>
                                </label>
                            ))
                        ) : (
                            <div className="sql-meta-empty">No lineage-based join candidates available.</div>
                        )}
                    </div>
                </div>

                <div className="action-row transformations-main-actions">
                    <button className="secondary-btn" onClick={handleResolveObjects} disabled={resolving || !readyForActions || !sqlPrompt.trim()} type="button">
                        {resolving ? "Resolving..." : "Resolve Objects"}
                    </button>

                    <button className="primary-btn" onClick={handleGenerateSql} disabled={sqlLoading || !readyForActions || !sqlPrompt.trim()} type="button">
                        {sqlLoading ? "Generating SQL..." : "Generate SQL"}
                    </button>

                    <button className="primary-btn" onClick={handlePreviewSql} disabled={!canPreview || previewLoading} type="button">
                        {previewLoading ? "Previewing..." : "Preview Result"}
                    </button>

                    <button className="primary-btn" onClick={handleSaveTransformation} disabled={!sqlResult?.sql} type="button">
                        Save Transformation
                    </button>

                    <button className="secondary-btn" onClick={handleGenerateDataQuality} disabled={dataQualityLoading} type="button">
                        {dataQualityLoading ? "Generating..." : "Data Quality SQL"}
                    </button>
                </div>

                {showDisambiguation && resolvedObjects ? (
                    <div className="disambiguation-panel">
                        <div className="disambiguation-header">
                            <h4>Resolved Objects from Your Prompt</h4>
                            <p className="section-caption">These are the database objects and columns AI will use. Confirm before generating SQL.</p>
                        </div>
                        {resolvedObjects.length === 0 ? (
                            <div className="disambiguation-empty">
                                No matching objects found in the schema for your prompt. Try rephrasing or check the object names.
                            </div>
                        ) : (
                            <div className="disambiguation-objects-grid">
                                {resolvedObjects.map((obj) => (
                                    <div key={`${obj.schema}.${obj.name}`} className="disambiguation-object-card">
                                        <div className="disambiguation-object-name">
                                            <span className="disambiguation-object-type">{obj.type || "TABLE"}</span>
                                            {obj.schema ? `${obj.schema}.${obj.name}` : obj.name}
                                        </div>
                                        <div className="disambiguation-column-list">
                                            {(obj.columns || []).map((col) => (
                                                <span key={col.column_name} className="disambiguation-column-chip">
                                                    {col.column_name}
                                                    <span className="disambiguation-col-type">{col.data_type}</span>
                                                </span>
                                            ))}
                                            {obj.columns?.length === 0 && (
                                                <span className="disambiguation-no-cols">No columns loaded</span>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                        <div className="disambiguation-actions">
                            <button className="primary-btn" onClick={handleConfirmAndGenerate} disabled={sqlLoading || resolvedObjects.length === 0} type="button">
                                {sqlLoading ? "Generating..." : "Confirm & Generate SQL"}
                            </button>
                            <button className="secondary-btn" onClick={handleDismissDisambiguation} type="button">
                                Dismiss
                            </button>
                        </div>
                    </div>
                ) : null}

                {sqlError ? <div className="object-details-error">{sqlError}</div> : null}
                {!canPreview && sqlResult?.sql ? (
                    <div className="object-details-empty" style={{ marginTop: "12px" }}>
                        Preview supports only SELECT / WITH queries.
                    </div>
                ) : null}
            </SectionCard>

            <SectionCard title="Generated SQL" subtitle="Generated SQL, assumptions, warnings, and LLM status.">
                {!sqlResult ? (
                    <div className="object-details-empty">No SQL generated yet.</div>
                ) : (
                    <div className="sql-result-panel">
                        <div className="sql-result-header">
                            <div>
                                <h3>{sqlResult.title || "Generated SQL"}</h3>
                                {sqlResult.explanation ? (
                                    <p className="section-caption sql-result-explanation" style={{ marginTop: "6px" }}>
                                        {sqlResult.explanation}
                                    </p>
                                ) : null}
                            </div>
                        </div>

                        <div className="form-field" style={{ marginTop: "12px" }}>
                            <label>Generated SQL</label>
                            <textarea
                                className="app-textarea sql-output sql-output-large"
                                value={sqlResult.sql || ""}
                                readOnly
                                rows={18}
                                style={{ width: "100%" }}
                            />
                        </div>

                        <div className="sql-meta-grid">
                            <div className="sql-meta-card">
                                <div className="sql-meta-title">Assumptions</div>
                                {normalizeList(sqlResult.assumptions).length ? (
                                    <ul className="sql-meta-list">
                                        {normalizeList(sqlResult.assumptions).map((item, idx) => (
                                            <li key={idx}>{item}</li>
                                        ))}
                                    </ul>
                                ) : (
                                    <div className="sql-meta-empty">No assumptions listed.</div>
                                )}
                            </div>

                            <div className="sql-meta-card">
                                <div className="sql-meta-title">Warnings</div>
                                {normalizeList(sqlResult.warnings).length ? (
                                    <ul className="sql-meta-list">
                                        {normalizeList(sqlResult.warnings).map((item, idx) => (
                                            <li key={idx}>{item}</li>
                                        ))}
                                    </ul>
                                ) : (
                                    <div className="sql-meta-empty">No warnings listed.</div>
                                )}
                            </div>
                        </div>

                        <div className="llm-status-banner">
                            LLM status: {sqlResult.llm_enabled ? "Enabled" : "Disabled"}
                            {sqlResult.llm_reason ? ` | ${sqlResult.llm_reason}` : ""}
                        </div>

                        {sqlResult.metadata_sent ? (
                            <div className="sql-meta-card" style={{ marginTop: "12px" }}>
                                <div className="sql-meta-title">Metadata Sent to AI</div>
                                <ul className="sql-meta-list">
                                    <li><strong>Base Object:</strong> {sqlResult.metadata_sent.base_object}</li>
                                    <li><strong>Columns:</strong> {(sqlResult.metadata_sent.base_columns || []).join(", ") || "none"}</li>
                                    {(sqlResult.metadata_sent.related_objects || []).length ? (
                                        <li><strong>Related Objects:</strong> {sqlResult.metadata_sent.related_objects.join(", ")}</li>
                                    ) : null}
                                    {(sqlResult.metadata_sent.cross_schema_refs || []).length ? (
                                        <li><strong>Cross-Schema Refs:</strong> {sqlResult.metadata_sent.cross_schema_refs.join(", ")}</li>
                                    ) : null}
                                </ul>
                            </div>
                        ) : null}
                    </div>
                )}
            </SectionCard>

            {previewError ? (
                <div className="object-details-error">{previewError}</div>
            ) : null}

            {previewResult ? (
                <SectionCard
                    title="Preview Result"
                    subtitle={`Showing top ${previewResult.rows?.length || 0} rows from a safe read-only preview.`}
                >
                    {previewResult.rows?.length ? (
                        <div className="sample-table-wrapper">
                            <table className="sample-table">
                                <thead>
                                    <tr>
                                        {previewResult.columns.map((col) => (
                                            <th key={col}>{col}</th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {previewResult.rows.map((row, rowIndex) => (
                                        <tr key={rowIndex}>
                                            {row.map((cell, cellIndex) => (
                                                <td key={cellIndex} title={formatPreviewCell(cell)}>
                                                    {formatPreviewCell(cell)}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    ) : (
                        <div className="object-details-empty">No rows returned for preview.</div>
                    )}
                </SectionCard>
            ) : null}

            {dataQualityResult ? (
                <SectionCard title="Data Quality SQL" subtitle="Reusable quality-check SQL generated for the current object.">
                    <div className="ai-suggestion-grid ai-suggestion-grid-stacked">
                        {dataQualityResult.checks?.map((item, idx) => (
                            <QualityCard key={`${item.title}-${idx}`} item={item} onUseSql={onUseQualitySql} />
                        ))}
                    </div>
                </SectionCard>
            ) : null}

            <SectionCard
                title="Saved Transformations"
                subtitle="Persistent backend-saved transformation templates for this object."
                actions={templatesLoading ? <div className="section-caption">Loading...</div> : null}
            >
                {savedTemplates.length ? (
                    <div className="ai-suggestion-grid ai-suggestion-grid-stacked">
                        {savedTemplates.map((item) => (
                            <TemplateCard
                                key={item.id}
                                item={item}
                                onReusePrompt={onReuseTemplatePrompt}
                                onDelete={onDeleteTemplate}
                            />
                        ))}
                    </div>
                ) : (
                    <div className="object-details-empty">No saved transformations yet.</div>
                )}
            </SectionCard>

            <SectionCard title="Prompt History" subtitle="Recent AI generation prompts for this object.">
                {promptHistory.length ? (
                    <div className="prompt-chip-row">
                        {promptHistory.map((item) => (
                            <PromptChip key={item.id} onClick={() => onUseHistoryPrompt(item)}>
                                {item.statement_type || "prompt"} · {(item.user_prompt || "").slice(0, 36)}
                            </PromptChip>
                        ))}
                    </div>
                ) : (
                    <div className="object-details-empty">No prompt history yet.</div>
                )}
            </SectionCard>
        </div>
    );
}

export default function LineagePage() {
    const {
        connectionId,
        setConnectionId,
        selectedDatabase: explorerSelectedDatabase,
        selectedSchema: explorerSelectedSchema,
        selectedObject: explorerSelectedObject,
        transformationCache,
        lineageUiSession,
        connectionPayload,
        sessionPassword,
        currentLineageKey,
        currentLineageActiveTab,
        currentLineageSelection,
        currentLineageViewSession,
        currentTransformationSession,
        currentPipelineBuilderSession,
        currentLineageUiSession,
        updateCurrentLineageSelection,
        updateCurrentLineageViewSession,
        updateCurrentTransformationSession,
        updateCurrentPipelineBuilderSession,
        setLineageActiveTab,
        setQueryRunnerQuery,
    } = useAppContext();

    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [selectorLoading, setSelectorLoading] = useState(false);
    const [selectorError, setSelectorError] = useState("");
    const [databaseOptions, setDatabaseOptions] = useState([]);
    const [schemaOptions, setSchemaOptions] = useState([]);
    const [objectOptions, setObjectOptions] = useState([]);
    const [selectorRefreshToken, setSelectorRefreshToken] = useState(0);
    const [sqlLoading, setSqlLoading] = useState(false);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [previewResult, setPreviewResult] = useState(null);
    const [previewError, setPreviewError] = useState("");
    const [savedTemplates, setSavedTemplates] = useState([]);
    const [templatesLoading, setTemplatesLoading] = useState(false);
    const [promptHistory, setPromptHistory] = useState([]);
    const [transformationSuggestions, setTransformationSuggestions] = useState({ summary: "", suggestions: [] });
    const [dataQualityLoading, setDataQualityLoading] = useState(false);
    const [dataQualityResult, setDataQualityResult] = useState(null);
    const [resolvedObjects, setResolvedObjects] = useState(null);
    const [resolving, setResolving] = useState(false);
    const [showDisambiguation, setShowDisambiguation] = useState(false);
    const [lineageGraphData, setLineageGraphData] = useState({
        nodes: [],
        edges: [],
        width: 920,
        height: 260,
        truncated: false,
    });
    const [lineageGraphLoading, setLineageGraphLoading] = useState(false);
    const [lineageGraphError, setLineageGraphError] = useState("");

    const selectedDatabase = currentLineageSelection?.selectedDatabase || "";
    const selectedSchema = currentLineageSelection?.selectedSchema || "";
    const selectedObject = currentLineageSelection?.selectedObject || "";

    const ready = useMemo(() => {
        return !!selectedDatabase && !!selectedSchema && !!selectedObject;
    }, [selectedDatabase, selectedSchema, selectedObject]);

    const activeTab = currentLineageActiveTab || currentLineageUiSession?.activeTab || "lineage";
    const lineage = currentLineageViewSession?.lineage || null;
    const lineageFetchedAt = currentLineageViewSession?.fetchedAt || null;
    const sqlPrompt = currentTransformationSession?.sqlPrompt || "";
    const sqlError = currentTransformationSession?.sqlError || "";
    const sqlResult = currentTransformationSession?.sqlResult || null;
    const selectedJoinTables = currentTransformationSession?.selectedJoinTables || [];

    const transformationsTabSelection =
        lineageUiSession?.tabSelections?.transformations || {
            selectedDatabase: "",
            selectedSchema: "",
            selectedObject: "",
        };

    const transformationsSessionKey = useMemo(() => {
        if (
            !transformationsTabSelection.selectedDatabase ||
            !transformationsTabSelection.selectedSchema ||
            !transformationsTabSelection.selectedObject
        ) {
            return "";
        }

        return `transformations|${transformationsTabSelection.selectedDatabase}.${transformationsTabSelection.selectedSchema}.${transformationsTabSelection.selectedObject}`;
    }, [transformationsTabSelection]);

    const emptyTransformationImportSession = useMemo(
        () => ({ sqlPrompt: "", sqlResult: null, sqlError: "", selectedJoinTables: [] }),
        []
    );

    const transformationsImportSession = useMemo(() => {
        if (!transformationsSessionKey) return emptyTransformationImportSession;
        return transformationCache[transformationsSessionKey] || emptyTransformationImportSession;
    }, [transformationsSessionKey, transformationCache, emptyTransformationImportSession]);

    const currentTransformationSqlForPipeline =
        transformationsImportSession?.sqlResult?.sql || "";

    const currentTransformationSource = transformationsSessionKey
        ? {
            databaseName: transformationsTabSelection.selectedDatabase,
            schemaName: transformationsTabSelection.selectedSchema,
            objectName: transformationsTabSelection.selectedObject,
            title: transformationsImportSession?.sqlResult?.title || "",
        }
        : null;

    const setActiveTab = (tab) => {
        setLineageActiveTab(tab, {
            selectedDatabase: selectedDatabase || explorerSelectedDatabase,
            selectedSchema: selectedSchema || explorerSelectedSchema,
            selectedObject: selectedObject || explorerSelectedObject,
        });
    };

    const setSqlPrompt = (nextValue) => {
        updateCurrentTransformationSession((prev) => ({
            ...prev,
            sqlPrompt: typeof nextValue === "function" ? nextValue(prev.sqlPrompt || "") : nextValue,
        }));
    };

    const setSqlError = (nextError) => {
        updateCurrentTransformationSession({ sqlError: nextError });
    };

    const setSqlResult = (nextResult) => {
        updateCurrentTransformationSession({ sqlResult: nextResult });
    };

    const setSelectedJoinTables = (nextValue) => {
        updateCurrentTransformationSession((prev) => ({
            ...prev,
            selectedJoinTables:
                typeof nextValue === "function"
                    ? nextValue(prev.selectedJoinTables || [])
                    : nextValue,
        }));
    };

    const updatePipelineBuilderSession = useCallback(
        (patch) => {
            updateCurrentPipelineBuilderSession(patch);
        },
        [updateCurrentPipelineBuilderSession]
    );

    const handleNavigateLineageObject = useCallback((item) => {
        if (!item?.schema_name || !item?.object_name) return;

        setError("");
        updateCurrentLineageSelection((prev) => ({
            ...prev,
            selectedSchema: item.schema_name,
            selectedObject: item.object_name,
        }));
    }, [updateCurrentLineageSelection]);

    useEffect(() => {
        if (ready) return;
        if (!explorerSelectedDatabase || !explorerSelectedSchema || !explorerSelectedObject) return;

        updateCurrentLineageSelection({
            selectedDatabase: explorerSelectedDatabase,
            selectedSchema: explorerSelectedSchema,
            selectedObject: explorerSelectedObject,
        });
    }, [
        ready,
        explorerSelectedDatabase,
        explorerSelectedSchema,
        explorerSelectedObject,
        updateCurrentLineageSelection,
    ]);

    const lineageTableOptions = useMemo(() => {
        if (!lineage) return [];

        const tables = new Set();
        tables.add(selectedObject);

        (lineage.upstream || []).forEach((item) => {
            if (item?.object_name) tables.add(item.object_name);
        });

        (lineage.downstream || []).forEach((item) => {
            if (item?.object_name) tables.add(item.object_name);
        });

        return Array.from(tables);
    }, [lineage, selectedObject]);

    useEffect(() => {
        if (!lineageTableOptions.length) {
            if ((selectedJoinTables || []).length) {
                setSelectedJoinTables([]);
            }
            return;
        }

        if ((selectedJoinTables || []).length) return;

        const defaults = lineageTableOptions
            .filter((name) => name !== selectedObject)
            .slice(0, 3);

        if (defaults.length) {
            setSelectedJoinTables(defaults);
        }
    }, [selectedObject, lineageTableOptions]);

    const ensureActiveBackendConnection = useCallback(async (forceNew = false) => {
        if (!forceNew && connectionId) {
            return connectionId;
        }

        if (!connectionPayload?.host || !connectionPayload?.username) {
            return null;
        }

        const payload = {
            ...connectionPayload,
            password: sessionPassword || "",
        };

        const saved = await saveConnection(payload);
        setConnectionId(saved.id);
        return saved.id;
    }, [connectionId, connectionPayload, sessionPassword, setConnectionId]);

    const runWithReconnect = useCallback(async (apiCall) => {
        let activeId = await ensureActiveBackendConnection(false);

        if (!activeId) {
            throw new Error("No active connection available");
        }

        try {
            return await apiCall(activeId);
        } catch (err) {
            const msg = err?.message || "";

            if (msg.includes("Connection not found")) {
                activeId = await ensureActiveBackendConnection(true);

                if (!activeId) {
                    throw new Error("No active connection available");
                }

                return await apiCall(activeId);
            }

            throw err;
        }
    }, [ensureActiveBackendConnection]);

    const buildTransitiveLineageGraph = useCallback(async () => {
        const rootKey = makeRelationKey(selectedSchema, selectedObject);
        if (!rootKey) {
            return {
                nodes: [],
                edges: [],
                width: 920,
                height: 260,
                truncated: false,
            };
        }

        const MAX_NODES = 80;
        const MAX_DEPTH = 5;
        const queue = [{ schemaName: selectedSchema, objectName: selectedObject, depth: 0 }];
        const visited = new Set();
        const nodeMap = new Map();
        const edgeMap = new Map();

        nodeMap.set(rootKey, {
            key: rootKey,
            schemaName: selectedSchema,
            objectName: selectedObject,
            objectType: lineage?.object_type || "OBJECT",
            isRoot: true,
        });

        let truncated = false;

        while (queue.length) {
            const current = queue.shift();
            const queueKey = makeRelationKey(current.schemaName, current.objectName);
            if (!queueKey || visited.has(queueKey)) continue;

            if (visited.size >= MAX_NODES) {
                truncated = true;
                break;
            }

            visited.add(queueKey);

            const lineageData = await runWithReconnect((activeId) =>
                fetchLineage(activeId, selectedDatabase, current.schemaName, current.objectName)
            );

            const canonicalSchema = lineageData?.schema_name || current.schemaName;
            const canonicalObject = lineageData?.object_name || current.objectName;
            const canonicalType = lineageData?.object_type || "OBJECT";
            const canonicalKey = makeRelationKey(canonicalSchema, canonicalObject);

            nodeMap.set(canonicalKey, {
                key: canonicalKey,
                schemaName: canonicalSchema,
                objectName: canonicalObject,
                objectType: canonicalType,
                isRoot: canonicalKey === rootKey,
            });

            const enqueueNode = (schemaName, objectName, depth) => {
                if (depth > MAX_DEPTH) return;
                const nextKey = makeRelationKey(schemaName, objectName);
                if (!nextKey || visited.has(nextKey)) return;
                queue.push({ schemaName, objectName, depth });
            };

            (lineageData?.upstream || []).forEach((item) => {
                const upstreamKey = makeRelationKey(item?.schema_name, item?.object_name);
                if (!upstreamKey) return;

                nodeMap.set(upstreamKey, {
                    key: upstreamKey,
                    schemaName: item.schema_name,
                    objectName: item.object_name,
                    objectType: item.object_type || "OBJECT",
                    isRoot: upstreamKey === rootKey,
                });

                const edgeKey = `${upstreamKey}->${canonicalKey}:${item.relation_kind || "related"}`;
                edgeMap.set(edgeKey, {
                    sourceKey: upstreamKey,
                    targetKey: canonicalKey,
                    relationKind: item.relation_kind || "related",
                });

                enqueueNode(item.schema_name, item.object_name, current.depth + 1);
            });

            (lineageData?.downstream || []).forEach((item) => {
                const downstreamKey = makeRelationKey(item?.schema_name, item?.object_name);
                if (!downstreamKey) return;

                nodeMap.set(downstreamKey, {
                    key: downstreamKey,
                    schemaName: item.schema_name,
                    objectName: item.object_name,
                    objectType: item.object_type || "OBJECT",
                    isRoot: downstreamKey === rootKey,
                });

                const edgeKey = `${canonicalKey}->${downstreamKey}:${item.relation_kind || "related"}`;
                edgeMap.set(edgeKey, {
                    sourceKey: canonicalKey,
                    targetKey: downstreamKey,
                    relationKind: item.relation_kind || "related",
                });

                enqueueNode(item.schema_name, item.object_name, current.depth + 1);
            });
        }

        const nodes = Array.from(nodeMap.values()).map((node) => ({
            ...node,
            isRoot: node.key === rootKey,
        }));

        const edges = Array.from(edgeMap.values());
        const layout = buildLineageGraphLayout(nodes, edges, rootKey);

        return {
            ...layout,
            truncated,
        };
    }, [lineage, runWithReconnect, selectedDatabase, selectedObject, selectedSchema]);

    const loadTemplates = async () => {
        if (!ready || !connectionId) return;
        setTemplatesLoading(true);
        try {
            const mod = await import("../api/schemaApi");
            const data = await mod.fetchTransformationRecipes(
                connectionId,
                selectedDatabase,
                selectedSchema,
                selectedObject
            );
            setSavedTemplates(data.recipes || []);
        } catch {
            setSavedTemplates([]);
        } finally {
            setTemplatesLoading(false);
        }
    };

    const loadPromptHistory = async () => {
        if (!ready || !connectionId) return;
        try {
            const mod = await import("../api/schemaApi");
            const data = await mod.fetchPromptHistory(
                connectionId,
                selectedDatabase,
                selectedSchema,
                selectedObject,
                10
            );
            setPromptHistory(data.items || []);
        } catch {
            setPromptHistory([]);
        }
    };

    const loadTransformationSuggestions = async () => {
        if (!ready || !connectionId) return;

        try {
            const [details, sample] = await Promise.all([
                fetchObjectDetails(connectionId, selectedDatabase, selectedSchema, selectedObject),
                fetchObjectSampleData(connectionId, selectedDatabase, selectedSchema, selectedObject, 25),
            ]);

            const payload = {
                object_name: selectedObject,
                schema_name: selectedSchema,
                database_name: selectedDatabase,
                columns: (details.columns || []).map((c) => ({
                    name: c.column_name,
                    data_type: c.data_type,
                })),
                sample_rows: sample.rows || [],
                user_prompt: "Suggest useful transformations, joins, standardization, and data quality ideas.",
                dialect: "postgres",
            };

            const suggestions = await getAITransformationSuggestions(payload);
            setTransformationSuggestions(suggestions || { summary: "", suggestions: [] });
        } catch {
            setTransformationSuggestions({ summary: "", suggestions: [] });
        }
    };

    const refreshLineage = useCallback(async () => {
        if (!ready) return;

        setLoading(true);
        setError("");

        try {
            const lineageData = await runWithReconnect((activeId) =>
                fetchLineage(activeId, selectedDatabase, selectedSchema, selectedObject)
            );

            updateCurrentLineageViewSession({
                lineage: lineageData,
                fetchedAt: new Date().toISOString(),
            });
        } catch (err) {
            setError(err.message || "Failed to fetch lineage");
        } finally {
            setLoading(false);
        }
    }, [
        ready,
        selectedDatabase,
        selectedSchema,
        selectedObject,
        updateCurrentLineageViewSession,
        connectionId,
        connectionPayload,
        sessionPassword,
    ]);

    useEffect(() => {
        let cancelled = false;

        if (!connectionPayload?.host || !connectionPayload?.username) {
            setDatabaseOptions([]);
            setSelectorError("");
            return undefined;
        }

        const loadDatabases = async () => {
            setSelectorLoading(true);
            setSelectorError("");

            try {
                const data = await runWithReconnect((activeId) => fetchDatabases(activeId));

                if (cancelled) return;
                setDatabaseOptions(Array.isArray(data?.databases) ? data.databases : []);
            } catch (err) {
                if (cancelled) return;
                setDatabaseOptions([]);
                setSelectorError(err.message || "Failed to load databases");
            } finally {
                if (!cancelled) {
                    setSelectorLoading(false);
                }
            }
        };

        loadDatabases();

        return () => {
            cancelled = true;
        };
    }, [
        connectionId,
        connectionPayload?.host,
        connectionPayload?.username,
        sessionPassword,
        selectorRefreshToken,
    ]);

    useEffect(() => {
        let cancelled = false;

        if (!selectedDatabase) {
            setSchemaOptions([]);
            setObjectOptions([]);
            return undefined;
        }

        const loadSchemasForSelection = async () => {
            setSelectorLoading(true);
            setSelectorError("");

            try {
                const data = await runWithReconnect((activeId) =>
                    fetchSchemas(activeId, selectedDatabase)
                );

                if (cancelled) return;
                setSchemaOptions(Array.isArray(data?.schemas) ? data.schemas : []);
            } catch (err) {
                if (cancelled) return;
                setSchemaOptions([]);
                setObjectOptions([]);
                setSelectorError(err.message || "Failed to load schemas");
            } finally {
                if (!cancelled) {
                    setSelectorLoading(false);
                }
            }
        };

        loadSchemasForSelection();

        return () => {
            cancelled = true;
        };
    }, [selectedDatabase, selectorRefreshToken, connectionId, sessionPassword]);

    useEffect(() => {
        let cancelled = false;

        if (!selectedDatabase || !selectedSchema) {
            setObjectOptions([]);
            return undefined;
        }

        const loadObjectsForSelection = async () => {
            setSelectorLoading(true);
            setSelectorError("");

            try {
                const data = await runWithReconnect((activeId) =>
                    fetchObjects(activeId, selectedDatabase, selectedSchema)
                );

                if (cancelled) return;
                setObjectOptions(
                    Array.isArray(data?.objects)
                        ? data.objects
                        : Array.isArray(data)
                            ? data
                            : []
                );
            } catch (err) {
                if (cancelled) return;
                setObjectOptions([]);
                setSelectorError(err.message || "Failed to load objects");
            } finally {
                if (!cancelled) {
                    setSelectorLoading(false);
                }
            }
        };

        loadObjectsForSelection();

        return () => {
            cancelled = true;
        };
    }, [selectedDatabase, selectedSchema, selectorRefreshToken, connectionId, sessionPassword]);

    useEffect(() => {
        if (!selectedDatabase || !databaseOptions.length) return;
        if (databaseOptions.includes(selectedDatabase)) return;

        updateCurrentLineageSelection({
            selectedDatabase: "",
            selectedSchema: "",
            selectedObject: "",
        });
    }, [databaseOptions, selectedDatabase, updateCurrentLineageSelection]);

    useEffect(() => {
        if (!selectedSchema || !schemaOptions.length) return;
        if (schemaOptions.includes(selectedSchema)) return;

        updateCurrentLineageSelection((prev) => ({
            ...prev,
            selectedSchema: "",
            selectedObject: "",
        }));
    }, [schemaOptions, selectedSchema, updateCurrentLineageSelection]);

    useEffect(() => {
        if (!selectedObject || !objectOptions.length) return;
        if (objectOptions.some((item) => item?.name === selectedObject)) return;

        updateCurrentLineageSelection((prev) => ({
            ...prev,
            selectedObject: "",
        }));
    }, [objectOptions, selectedObject, updateCurrentLineageSelection]);

    useEffect(() => {
        if (activeTab !== "lineage") return;

        if (!ready) {
            setError("");
            return;
        }

        refreshLineage();
    }, [activeTab, ready, currentLineageKey, refreshLineage]);

    useEffect(() => {
        let cancelled = false;

        if (activeTab !== "lineage" || !ready) {
            setLineageGraphError("");
            setLineageGraphData({
                nodes: [],
                edges: [],
                width: 920,
                height: 260,
                truncated: false,
            });
            return undefined;
        }

        const loadLineageGraph = async () => {
            setLineageGraphLoading(true);
            setLineageGraphError("");

            try {
                const graph = await buildTransitiveLineageGraph();
                if (cancelled) return;
                setLineageGraphData(graph);
            } catch (err) {
                if (cancelled) return;
                setLineageGraphError(err.message || "Failed to build lineage graph");
            } finally {
                if (!cancelled) {
                    setLineageGraphLoading(false);
                }
            }
        };

        loadLineageGraph();

        return () => {
            cancelled = true;
        };
    }, [
        activeTab,
        ready,
        currentLineageKey,
        lineageFetchedAt,
        buildTransitiveLineageGraph,
    ]);

    useEffect(() => {
        if (activeTab !== "transformations") return;
        if (!ready || !connectionId) return;

        loadTemplates();
        loadPromptHistory();
        loadTransformationSuggestions();
    }, [
        activeTab,
        ready,
        connectionId,
        selectedDatabase,
        selectedSchema,
        selectedObject,
    ]);

    const handleResolveObjects = async () => {
        if (!sqlPrompt.trim()) {
            setSqlError("Please enter a prompt before resolving objects.");
            return;
        }

        setResolving(true);
        setSqlError("");
        setResolvedObjects(null);
        setShowDisambiguation(false);

        try {
            const data = await runWithReconnect((activeId) =>
                resolveObjects(activeId, selectedDatabase, selectedSchema, sqlPrompt)
            );
            setResolvedObjects(data.matched_objects || []);
            setShowDisambiguation(true);
        } catch (err) {
            setSqlError(err.message || "Failed to resolve objects");
        } finally {
            setResolving(false);
        }
    };

    const handleDismissDisambiguation = () => {
        setShowDisambiguation(false);
        setResolvedObjects(null);
    };

    const handleGenerateSql = async () => {
        if (!sqlPrompt.trim()) {
            setSqlError("Please enter a prompt before generating SQL.");
            return;
        }

        setSqlLoading(true);
        setSqlError("");
        setSqlResult(null);
        setPreviewResult(null);
        setPreviewError("");

        try {
            const data = await runWithReconnect((activeId) =>
                generateAiSql(
                    activeId,
                    selectedDatabase,
                    selectedSchema,
                    selectedObject,
                    sqlPrompt,
                    selectedJoinTables
                )
            );

            updateCurrentTransformationSession({
                sqlResult: data,
                sqlError: "",
            });

            loadPromptHistory();
        } catch (err) {
            setSqlError(err.message || "Failed to generate SQL");
        } finally {
            setSqlLoading(false);
        }
    };

    const handleConfirmAndGenerate = async () => {
        setShowDisambiguation(false);
        await handleGenerateSql();
    };

    const handleClearPrompt = () => {
        updateCurrentTransformationSession({
            sqlPrompt: "",
            sqlError: "",
        });
        setPreviewResult(null);
        setPreviewError("");
    };

    const handleCopyPrompt = async () => {
        if (!sqlPrompt?.trim()) return;
        try {
            await navigator.clipboard.writeText(sqlPrompt);
        } catch (err) {
            console.error("Failed to copy prompt:", err);
        }
    };

    const handlePreviewSql = async () => {
        if (!sqlResult?.sql) return;

        if (!isPreviewableSql(sqlResult.sql)) {
            setPreviewResult(null);
            setPreviewError("Preview supports only SELECT / WITH queries.");
            return;
        }

        setPreviewLoading(true);
        setPreviewError("");
        setPreviewResult(null);

        try {
            const data = await runWithReconnect((activeId) =>
                executeQuery(activeId, selectedDatabase, {
                    query: sqlResult.sql,
                    limit: 25,
                    offset: 0,
                })
            );

            setPreviewResult(data);
        } catch (err) {
            setPreviewError(err.message || "Failed to preview SQL");
        } finally {
            setPreviewLoading(false);
        }
    };

    const handleSaveTransformation = async () => {
        if (!sqlResult?.sql || !connectionId) return;

        try {
            await saveTransformationRecipe({
                connection_id: connectionId,
                database_name: selectedDatabase,
                schema: selectedSchema,
                object_name: selectedObject,
                recipe_name: sqlResult.title || `${selectedObject} transformation`,
                user_prompt: sqlPrompt,
                statement_type: sqlResult.statement_type || "select",
                selected_tables: selectedJoinTables,
                sql: sqlResult.sql,
                explanation: sqlResult.explanation || "",
            });
            loadTemplates();
        } catch (err) {
            setSqlError(err.message || "Failed to save transformation");
        }
    };

    const handleDeleteTemplate = async (item) => {
        try {
            await deleteTransformationRecipe(item.id);
            loadTemplates();
        } catch (err) {
            console.error("Failed to delete transformation", err);
        }
    };

    const handleReuseTemplatePrompt = (item) => {
        setSqlPrompt(item.user_prompt || "");
    };

    const handleUseHistoryPrompt = (item) => {
        setSqlPrompt(item.user_prompt || "");
    };

    const handleAppendSuggestionPrompt = (item) => {
        const text = `Include transformation: ${item.title}. Reason: ${item.reason || ""}`;
        setSqlPrompt((prev) => (prev?.trim() ? `${prev.trim()}\n\n${text}` : text));
    };

    const handleGenerateDataQuality = async () => {
        if (!connectionId) return;

        setDataQualityLoading(true);
        try {
            const data = await generateDataQualitySql(
                connectionId,
                selectedDatabase,
                selectedSchema,
                selectedObject
            );
            setDataQualityResult(data);
        } catch (err) {
            setSqlError(err.message || "Failed to generate data quality SQL");
        } finally {
            setDataQualityLoading(false);
        }
    };

    const handleUseQualitySql = (sql) => {
        if (!sql) return;
        setQueryRunnerQuery(sql);
    };

    const toggleJoinTable = (tableName) => {
        if (tableName === selectedObject) return;

        setSelectedJoinTables((prev) => {
            if (prev.includes(tableName)) {
                return prev.filter((t) => t !== tableName);
            }
            return [...prev, tableName];
        });
    };

    const selectAllJoinTables = () => {
        setSelectedJoinTables(
            lineageTableOptions.filter((tableName) => tableName !== selectedObject)
        );
    };

    const clearJoinTables = () => {
        setSelectedJoinTables([]);
    };

    const readyForActions =
        ready && !!connectionPayload?.host && !!connectionPayload?.username;

    return (
        <div className="module-shell">
            <div className="module-hero">
                <div>
                    <div className="module-badge">Engineering Flow</div>
                    <h1 className="module-title">ETL / Lineage</h1>
                    <p className="module-subtitle">
                        Understand dependency flow in Lineage, build reusable transformation logic,
                        and orchestrate execution with Pipeline Builder.
                    </p>
                </div>
            </div>

            <div className="lineage-selection-section">
                <SectionCard
                    title="Object Selection"
                    subtitle="Explorer can seed this context automatically, but you can also choose the object directly here."
                    actions={(
                        <div className="lineage-selector-actions">
                            <button
                                type="button"
                                className="secondary-btn"
                                onClick={() => setSelectorRefreshToken((prev) => prev + 1)}
                                disabled={selectorLoading}
                            >
                                {selectorLoading ? "Loading..." : "Reload Options"}
                            </button>

                            <button
                                type="button"
                                className="secondary-btn"
                                onClick={refreshLineage}
                                disabled={!ready || loading}
                            >
                                {loading ? "Refreshing..." : "Refresh Lineage"}
                            </button>
                        </div>
                    )}
                >
                    <div className="lineage-selector-grid">
                        <div className="form-field">
                            <label>Database</label>
                            <select value={selectedDatabase} onChange={(e) => {
                                setError("");
                                updateCurrentLineageSelection({
                                    selectedDatabase: e.target.value,
                                    selectedSchema: "",
                                    selectedObject: "",
                                });
                            }}>
                                <option value="">Select a database</option>
                                {databaseOptions.map((databaseName) => (
                                    <option key={databaseName} value={databaseName}>
                                        {databaseName}
                                    </option>
                                ))}
                            </select>
                        </div>

                        <div className="form-field">
                            <label>Schema</label>
                            <select
                                value={selectedSchema}
                                onChange={(e) => {
                                    setError("");
                                    updateCurrentLineageSelection((prev) => ({
                                        ...prev,
                                        selectedSchema: e.target.value,
                                        selectedObject: "",
                                    }));
                                }}
                                disabled={!selectedDatabase}
                            >
                                <option value="">Select a schema</option>
                                {schemaOptions.map((schemaName) => (
                                    <option key={schemaName} value={schemaName}>
                                        {schemaName}
                                    </option>
                                ))}
                            </select>
                        </div>

                        <div className="form-field">
                            <label>Object</label>
                            <select
                                value={selectedObject}
                                onChange={(e) => {
                                    setError("");
                                    updateCurrentLineageSelection((prev) => ({
                                        ...prev,
                                        selectedObject: e.target.value,
                                    }));
                                }}
                                disabled={!selectedDatabase || !selectedSchema}
                            >
                                <option value="">Select an object</option>
                                {objectOptions.map((item) => (
                                    <option key={item.name} value={item.name}>
                                        {item.name}{item.type ? ` (${item.type})` : ""}
                                    </option>
                                ))}
                            </select>
                        </div>
                    </div>

                    {selectorError ? <div className="object-details-error">{selectorError}</div> : null}
                </SectionCard>
            </div>

            <div className="etl-tab-bar">
                <TabButton active={activeTab === "lineage"} onClick={() => setActiveTab("lineage")}>
                    Lineage
                </TabButton>

                <TabButton active={activeTab === "transformations"} onClick={() => setActiveTab("transformations")}>
                    Transformations
                </TabButton>

                <TabButton active={activeTab === "pipeline-builder"} onClick={() => setActiveTab("pipeline-builder")}>
                    Pipeline Builder
                </TabButton>
            </div>

            {!ready ? (
                <div className="lineage-empty-state">
                    <div className="empty-title">
                        {activeTab === "lineage"
                            ? "Select an object to inspect lineage"
                            : activeTab === "transformations"
                                ? "Select an object to build transformations"
                                : "Select an object to configure a pipeline"}
                    </div>
                    <div className="empty-subtitle">
                        Choose a database, schema, and object above. Explorer selection still carries over when available.
                    </div>
                </div>
            ) : (
                <>
                    <div className="context-bar">
                        <div className="context-pill">DB: {selectedDatabase}</div>
                        <div className="context-pill">Schema: {selectedSchema}</div>
                        <div className="context-pill">Object: {selectedObject}</div>
                    </div>

                    {activeTab === "lineage" && loading ? <div className="loading-banner">Loading lineage...</div> : null}
                    {activeTab === "lineage" && error ? <div className="object-details-error">{error}</div> : null}

                    {activeTab === "lineage" ? (
                        lineage ? (
                            <LineageTab
                                lineage={lineage}
                                graphData={lineageGraphData}
                                graphLoading={lineageGraphLoading}
                                graphError={lineageGraphError}
                                onNavigate={handleNavigateLineageObject}
                            />
                        ) : !loading && !error ? (
                            <div className="lineage-empty-state">
                                <div className="empty-title">No lineage found for the selected object</div>
                                <div className="empty-subtitle">
                                    Try Refresh Lineage, or choose another object if this one has no captured upstream or downstream dependencies yet.
                                </div>
                            </div>
                        ) : null
                    ) : null}

                    {activeTab === "transformations" ? (
                        <TransformationsTab
                            sqlPrompt={sqlPrompt}
                            setSqlPrompt={setSqlPrompt}
                            lineageTableOptions={lineageTableOptions}
                            selectedJoinTables={selectedJoinTables}
                            toggleJoinTable={toggleJoinTable}
                            selectAllJoinTables={selectAllJoinTables}
                            clearJoinTables={clearJoinTables}
                            handleGenerateSql={handleGenerateSql}
                            handleResolveObjects={handleResolveObjects}
                            handleDismissDisambiguation={handleDismissDisambiguation}
                            handleConfirmAndGenerate={handleConfirmAndGenerate}
                            resolvedObjects={resolvedObjects}
                            resolving={resolving}
                            showDisambiguation={showDisambiguation}
                            handleClearPrompt={handleClearPrompt}
                            handleCopyPrompt={handleCopyPrompt}
                            handlePreviewSql={handlePreviewSql}
                            handleSaveTransformation={handleSaveTransformation}
                            handleGenerateDataQuality={handleGenerateDataQuality}
                            sqlLoading={sqlLoading}
                            sqlError={sqlError}
                            sqlResult={sqlResult}
                            previewLoading={previewLoading}
                            previewResult={previewResult}
                            previewError={previewError}
                            readyForActions={readyForActions}
                            selectedObject={selectedObject}
                            selectedSchema={selectedSchema}
                            selectedDatabase={selectedDatabase}
                            savedTemplates={savedTemplates}
                            promptHistory={promptHistory}
                            transformationSuggestions={transformationSuggestions}
                            dataQualityResult={dataQualityResult}
                            dataQualityLoading={dataQualityLoading}
                            templatesLoading={templatesLoading}
                            onReuseTemplatePrompt={handleReuseTemplatePrompt}
                            onDeleteTemplate={handleDeleteTemplate}
                            onUseHistoryPrompt={handleUseHistoryPrompt}
                            onAppendSuggestionPrompt={handleAppendSuggestionPrompt}
                            onUseQualitySql={handleUseQualitySql}
                        />
                    ) : null}

                    {activeTab === "pipeline-builder" ? (
                        <PipelineBuilder
                            sessionKey={currentLineageKey}
                            connectionId={connectionId}
                            databaseName={selectedDatabase}
                            schemaName={selectedSchema}
                            selectedObject={selectedObject}
                            currentTransformationSql={currentTransformationSqlForPipeline}
                            currentTransformationSource={currentTransformationSource}
                            pipelineSession={currentPipelineBuilderSession}
                            updatePipelineSession={updatePipelineBuilderSession}
                        />
                    ) : null}
                </>
            )}
        </div>
    );
}