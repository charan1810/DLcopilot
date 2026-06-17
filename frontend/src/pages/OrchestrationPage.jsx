import { useEffect, useMemo, useRef, useState } from "react";
import {
    createWorkflow,
    createWorkflowSchedule,
    deleteWorkflow,
    deleteWorkflowSchedule,
    fetchPipelines,
    fetchWorkflowRun,
    fetchWorkflowRuns,
    fetchWorkflowSchedule,
    fetchWorkflows,
    getWorkflow,
    runWorkflow,
    updateWorkflow,
    updateWorkflowGraph,
    updateWorkflowSchedule,
} from "../api/schemaApi";
import { useAppContext } from "../context/AppContext";
import "./OrchestrationPage.css";

const DEFAULT_NEW_WORKFLOW_NAME = "";
const DEFAULT_SCHEDULE_FORM = {
    schedule_type: "interval",
    interval_minutes: 30,
    cron_expression: "0 * * * *",
    is_active: true,
};

const NODE_WIDTH = 224;
const NODE_HEIGHT = 54;
const GRAPH_SIZE = 5000;
const ZOOM_MIN = 0.45;
const ZOOM_MAX = 1.8;

function normalizeNodes(nodes = []) {
    return nodes.map((node, idx) => ({
        ...node,
        pos_x: Number(node.pos_x ?? 120 + (idx % 3) * 260),
        pos_y: Number(node.pos_y ?? 120 + Math.floor(idx / 3) * 140),
    }));
}

function nodeLabel(node) {
    return node.node_name || node.pipeline_name || `Pipeline ${node.pipeline_id}`;
}

function makeTempId() {
    return -Math.floor(Math.random() * 1_000_000);
}

function parseQualifiedSchema(value = "") {
    const cleaned = String(value || "").trim().replace(/^"+|"+$/g, "");
    if (!cleaned) return "";
    if (!cleaned.includes(".")) return "";
    const [schema] = cleaned.split(".", 1);
    return schema ? schema.trim() : "";
}

function parseQualifiedObject(value = "") {
    const cleaned = String(value || "").trim().replace(/^"+|"+$/g, "");
    if (!cleaned) return "";
    const parts = cleaned.split(".");
    return parts.length > 1 ? parts.slice(1).join(".").trim() : cleaned;
}

function inferSourceSchema(pipeline) {
    const fromSourceObject = parseQualifiedSchema(pipeline?.source_object || "");
    if (fromSourceObject && fromSourceObject !== "*") {
        return fromSourceObject.toUpperCase();
    }

    const fromSchemaName = String(pipeline?.schema_name || "").trim();
    if (fromSchemaName) {
        return fromSchemaName.toUpperCase();
    }

    const fromName = String(pipeline?.name || "").toLowerCase();
    if (fromName.includes("src")) return "SRC";
    if (fromName.includes("core")) return "CORE";
    if (fromName.includes("rpt") || fromName.includes("report")) return "RPT";

    return "UNKNOWN";
}

function inferTargetSchema(pipeline) {
    const fromTargetObject = parseQualifiedSchema(pipeline?.target_object || "");
    if (fromTargetObject && fromTargetObject !== "*") {
        return fromTargetObject.toUpperCase();
    }

    const fromName = String(pipeline?.name || "").toLowerCase();
    if (fromName.includes("to_rpt") || fromName.includes("to_report")) return "RPT";
    if (fromName.includes("to_core")) return "CORE";
    if (fromName.includes("to_src")) return "SRC";

    return "TARGET";
}

function getPipelineFlowGroup(pipeline) {
    return `${inferSourceSchema(pipeline)} -> ${inferTargetSchema(pipeline)}`;
}

export default function OrchestrationPage() {
    const { connectionId, connectionPayload, selectedDatabase } = useAppContext();
    const databaseName = selectedDatabase || connectionPayload?.database_name || "dlcopilot";

    const [pipelines, setPipelines] = useState([]);
    const [pipelineSearch, setPipelineSearch] = useState("");

    const [workflows, setWorkflows] = useState([]);
    const [selectedWorkflowId, setSelectedWorkflowId] = useState(null);
    const [workflow, setWorkflow] = useState(null);

    const [newWorkflowName, setNewWorkflowName] = useState(DEFAULT_NEW_WORKFLOW_NAME);
    const [linkMode, setLinkMode] = useState(false);
    const [linkSourceNodeId, setLinkSourceNodeId] = useState(null);

    const [schedule, setSchedule] = useState(null);
    const [scheduleForm, setScheduleForm] = useState(DEFAULT_SCHEDULE_FORM);

    const [workflowRuns, setWorkflowRuns] = useState([]);
    const [selectedRun, setSelectedRun] = useState(null);

    const [loading, setLoading] = useState(false);
    const [savingGraph, setSavingGraph] = useState(false);
    const [running, setRunning] = useState(false);
    const [canvasDragOver, setCanvasDragOver] = useState(false);
    const [view, setView] = useState({ zoom: 1, offsetX: 0, offsetY: 0 });
    const [error, setError] = useState("");
    const [message, setMessage] = useState("");

    const canvasRef = useRef(null);
    const miniMapRef = useRef(null);
    const dragRef = useRef({ active: false, nodeId: null, offsetX: 0, offsetY: 0 });

    const hasConnectionContext = !!connectionId;

    const workflowNodes = useMemo(() => normalizeNodes(workflow?.nodes || []), [workflow]);
    const workflowEdges = useMemo(() => workflow?.edges || [], [workflow]);

    const nodeById = useMemo(() => {
        const map = new Map();
        workflowNodes.forEach((node) => map.set(Number(node.id), node));
        return map;
    }, [workflowNodes]);

    const graphBounds = useMemo(() => {
        if (!workflowNodes.length) {
            return { minX: 0, minY: 0, maxX: NODE_WIDTH, maxY: NODE_HEIGHT };
        }

        let minX = Number.POSITIVE_INFINITY;
        let minY = Number.POSITIVE_INFINITY;
        let maxX = Number.NEGATIVE_INFINITY;
        let maxY = Number.NEGATIVE_INFINITY;

        workflowNodes.forEach((node) => {
            minX = Math.min(minX, Number(node.pos_x));
            minY = Math.min(minY, Number(node.pos_y));
            maxX = Math.max(maxX, Number(node.pos_x) + NODE_WIDTH);
            maxY = Math.max(maxY, Number(node.pos_y) + NODE_HEIGHT);
        });

        return { minX, minY, maxX, maxY };
    }, [workflowNodes]);

    const clampZoom = (zoom) => Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, zoom));

    const screenToWorld = (clientX, clientY) => {
        const canvas = canvasRef.current;
        if (!canvas) {
            return { x: 0, y: 0 };
        }

        const bounds = canvas.getBoundingClientRect();
        return {
            x: (clientX - bounds.left - view.offsetX) / view.zoom,
            y: (clientY - bounds.top - view.offsetY) / view.zoom,
        };
    };

    const fitGraphToCanvas = () => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        const canvasWidth = canvas.clientWidth;
        const canvasHeight = canvas.clientHeight;

        const graphWidth = Math.max(1, graphBounds.maxX - graphBounds.minX);
        const graphHeight = Math.max(1, graphBounds.maxY - graphBounds.minY);

        const nextZoom = clampZoom(
            Math.min(
                (canvasWidth - 80) / graphWidth,
                (canvasHeight - 80) / graphHeight,
                1.25
            )
        );

        const offsetX = (canvasWidth - graphWidth * nextZoom) / 2 - graphBounds.minX * nextZoom;
        const offsetY = (canvasHeight - graphHeight * nextZoom) / 2 - graphBounds.minY * nextZoom;

        setView({ zoom: nextZoom, offsetX, offsetY });
    };

    const getWorldViewport = () => {
        const canvas = canvasRef.current;
        if (!canvas) return null;

        const width = canvas.clientWidth;
        const height = canvas.clientHeight;

        return {
            x: -view.offsetX / view.zoom,
            y: -view.offsetY / view.zoom,
            width: width / view.zoom,
            height: height / view.zoom,
        };
    };

    const miniMapModel = useMemo(() => {
        const mapWidth = 220;
        const mapHeight = 130;

        const graphWidth = Math.max(1, graphBounds.maxX - graphBounds.minX);
        const graphHeight = Math.max(1, graphBounds.maxY - graphBounds.minY);
        const scale = Math.min((mapWidth - 16) / graphWidth, (mapHeight - 16) / graphHeight);

        const projectX = (x) => 8 + (x - graphBounds.minX) * scale;
        const projectY = (y) => 8 + (y - graphBounds.minY) * scale;

        return {
            mapWidth,
            mapHeight,
            scale,
            projectX,
            projectY,
        };
    }, [graphBounds]);

    const filteredPipelines = useMemo(() => {
        const q = pipelineSearch.trim().toLowerCase();
        if (!q) return pipelines;
        return pipelines.filter((pipeline) => {
            const text = `${pipeline.id} ${pipeline.name || ""} ${pipeline.source_object || ""} ${pipeline.target_object || ""}`.toLowerCase();
            return text.includes(q);
        });
    }, [pipelines, pipelineSearch]);

    const pipelineTree = useMemo(() => {
        const tree = new Map();

        filteredPipelines.forEach((pipeline) => {
            const dbName = String(pipeline.database_name || databaseName || "unknown_db").trim() || "unknown_db";
            const sourceSchema = parseQualifiedSchema(pipeline.source_object || "");
            const targetSchema = parseQualifiedSchema(pipeline.target_object || "");
            const explicitSchema = String(pipeline.schema_name || "").trim();
            const schemaName = (
                sourceSchema
                || explicitSchema
                || targetSchema
                || inferSourceSchema(pipeline).toLowerCase()
                || "unknown_schema"
            );
            const objectName = (
                parseQualifiedObject(pipeline.source_object || "")
                || String(pipeline.source_object || "").trim()
                || "*"
            );

            if (!tree.has(dbName)) {
                tree.set(dbName, new Map());
            }
            const schemaMap = tree.get(dbName);

            if (!schemaMap.has(schemaName)) {
                schemaMap.set(schemaName, new Map());
            }
            const objectMap = schemaMap.get(schemaName);

            if (!objectMap.has(objectName)) {
                objectMap.set(objectName, []);
            }
            objectMap.get(objectName).push(pipeline);
        });

        return Array.from(tree.entries())
            .map(([dbName, schemaMap]) => {
                const schemas = Array.from(schemaMap.entries())
                    .map(([schemaName, objectMap]) => {
                        const objects = Array.from(objectMap.entries())
                            .map(([objectName, items]) => ({
                                objectName,
                                items: items.slice().sort((a, b) => String(a.name || "").localeCompare(String(b.name || ""))),
                            }))
                            .sort((a, b) => a.objectName.localeCompare(b.objectName));

                        const count = objects.reduce((sum, obj) => sum + obj.items.length, 0);
                        return { schemaName, objects, count };
                    })
                    .sort((a, b) => a.schemaName.localeCompare(b.schemaName));

                const count = schemas.reduce((sum, schema) => sum + schema.count, 0);
                return { dbName, schemas, count };
            })
            .sort((a, b) => a.dbName.localeCompare(b.dbName));
    }, [filteredPipelines, databaseName]);

    const setWorkflowLocal = (updater) => {
        setWorkflow((prev) => {
            if (!prev) return prev;
            return typeof updater === "function" ? updater(prev) : { ...prev, ...updater };
        });
    };

    const resetLinkState = () => {
        setLinkSourceNodeId(null);
    };

    const loadPipelines = async () => {
        if (!hasConnectionContext) return;

        const [scopedRows, connectionRows, allRows] = await Promise.all([
            fetchPipelines({ connection_id: connectionId, database_name: databaseName }).catch(() => []),
            fetchPipelines({ connection_id: connectionId }).catch(() => []),
            fetchPipelines({}).catch(() => []),
        ]);

        const merged = [];
        const seen = new Set();

        [scopedRows, connectionRows, allRows].forEach((rows) => {
            (rows || []).forEach((pipeline) => {
                const id = Number(pipeline?.id);
                if (!id || seen.has(id)) return;
                seen.add(id);
                merged.push(pipeline);
            });
        });

        setPipelines(merged);
    };

    const loadWorkflowRuns = async (workflowId) => {
        if (!workflowId) {
            setWorkflowRuns([]);
            return;
        }
        const rows = await fetchWorkflowRuns(workflowId);
        setWorkflowRuns(rows || []);
    };

    const loadWorkflowSchedule = async (workflowId) => {
        if (!workflowId) {
            setSchedule(null);
            return;
        }
        const res = await fetchWorkflowSchedule(workflowId);
        const scheduleObj = res?.schedule || res || null;
        setSchedule(scheduleObj);

        if (scheduleObj?.id) {
            setScheduleForm({
                schedule_type: scheduleObj.schedule_type || "interval",
                interval_minutes: scheduleObj.interval_minutes || 30,
                cron_expression: scheduleObj.cron_expression || "0 * * * *",
                is_active: scheduleObj.is_active !== false,
            });
        } else {
            setScheduleForm(DEFAULT_SCHEDULE_FORM);
        }
    };

    const loadWorkflows = async (selectId = null) => {
        if (!hasConnectionContext) return;

        let rows = await fetchWorkflows({ connection_id: connectionId, database_name: databaseName });
        if (!Array.isArray(rows) || rows.length === 0) {
            rows = await fetchWorkflows({ connection_id: connectionId });
        }
        if (!Array.isArray(rows) || rows.length === 0) {
            rows = await fetchWorkflows({});
        }

        setWorkflows(rows || []);

        const nextId = selectId ?? selectedWorkflowId ?? rows?.[0]?.id ?? null;
        setSelectedWorkflowId(nextId);
        resetLinkState();

        if (!nextId) {
            setWorkflow(null);
            setSelectedRun(null);
            await Promise.all([loadWorkflowRuns(null), loadWorkflowSchedule(null)]);
            return;
        }

        const details = await getWorkflow(nextId);
        setWorkflow({ ...details, nodes: normalizeNodes(details.nodes || []) });
        await Promise.all([loadWorkflowRuns(nextId), loadWorkflowSchedule(nextId)]);
    };

    const refreshAll = async (workflowId = selectedWorkflowId) => {
        setLoading(true);
        setError("");
        try {
            await loadPipelines();
            await loadWorkflows(workflowId);
        } catch (err) {
            setError(err?.message || "Failed to load workflow studio data.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        let cancelled = false;

        async function hydrate() {
            if (!hasConnectionContext) return;
            setLoading(true);
            setError("");
            try {
                await loadPipelines();
                if (!cancelled) await loadWorkflows();
            } catch (err) {
                if (!cancelled) setError(err?.message || "Failed to load workflow studio.");
            } finally {
                if (!cancelled) setLoading(false);
            }
        }

        hydrate();

        return () => {
            cancelled = true;
        };
    }, [hasConnectionContext, connectionId, databaseName]);

    useEffect(() => {
        if (!workflowNodes.length) {
            setView({ zoom: 1, offsetX: 0, offsetY: 0 });
            return;
        }

        const timer = window.setTimeout(() => fitGraphToCanvas(), 0);
        return () => window.clearTimeout(timer);
    }, [selectedWorkflowId]);

    const handleCreateWorkflow = async () => {
        const name = newWorkflowName.trim();
        if (!name) {
            setError("Workflow name is required.");
            return;
        }

        setLoading(true);
        setError("");
        setMessage("");
        try {
            const created = await createWorkflow({
                name,
                description: "",
                connection_id: connectionId,
                database_name: databaseName,
                execution_mode: "serial",
                stop_on_error: true,
                is_active: true,
            });
            setNewWorkflowName(DEFAULT_NEW_WORKFLOW_NAME);
            setMessage("Workflow created.");
            await refreshAll(created.id);
        } catch (err) {
            setError(err?.message || "Failed to create workflow.");
        } finally {
            setLoading(false);
        }
    };

    const handleSaveWorkflowMeta = async () => {
        if (!workflow) return;

        setLoading(true);
        setError("");
        setMessage("");
        try {
            await updateWorkflow(workflow.id, {
                name: workflow.name,
                description: workflow.description || "",
                connection_id: workflow.connection_id,
                database_name: workflow.database_name || "",
                execution_mode: workflow.execution_mode || "serial",
                stop_on_error: workflow.stop_on_error !== false,
                is_active: workflow.is_active !== false,
            });
            setMessage("Workflow settings saved.");
            await refreshAll(workflow.id);
        } catch (err) {
            setError(err?.message || "Failed to save workflow settings.");
        } finally {
            setLoading(false);
        }
    };

    const handleDeleteWorkflow = async () => {
        if (!workflow) return;
        if (!window.confirm(`Delete workflow \"${workflow.name}\"?`)) return;

        setLoading(true);
        setError("");
        setMessage("");
        try {
            await deleteWorkflow(workflow.id);
            setMessage("Workflow deleted.");
            await refreshAll(null);
        } catch (err) {
            setError(err?.message || "Failed to delete workflow.");
        } finally {
            setLoading(false);
        }
    };

    const addNodeFromPipeline = (pipelineId, dropX = null, dropY = null) => {
        if (!workflow) {
            setError("Create or select a workflow first.");
            return;
        }

        const numericPipelineId = Number(pipelineId || 0);
        if (!numericPipelineId) return;

        const pipeline = pipelines.find((item) => Number(item.id) === numericPipelineId);
        const index = workflowNodes.length;

        const posX = Number.isFinite(dropX) ? dropX : 80 + (index % 3) * 260;
        const posY = Number.isFinite(dropY) ? dropY : 90 + Math.floor(index / 3) * 140;

        setWorkflowLocal((prev) => ({
            ...prev,
            nodes: [
                ...(prev.nodes || []),
                {
                    id: makeTempId(),
                    pipeline_id: numericPipelineId,
                    pipeline_name: pipeline?.name || `Pipeline ${numericPipelineId}`,
                    node_name: pipeline?.name || "",
                    pos_x: posX,
                    pos_y: posY,
                },
            ],
        }));

        setMessage(`Added pipeline ${numericPipelineId} to canvas.`);
    };

    const removeNode = (nodeId) => {
        setWorkflowLocal((prev) => ({
            ...prev,
            nodes: (prev.nodes || []).filter((node) => Number(node.id) !== Number(nodeId)),
            edges: (prev.edges || []).filter(
                (edge) => Number(edge.from_node_id) !== Number(nodeId) && Number(edge.to_node_id) !== Number(nodeId)
            ),
        }));

        if (Number(linkSourceNodeId) === Number(nodeId)) {
            resetLinkState();
        }
    };

    const addEdge = (fromNodeId, toNodeId) => {
        const fromId = Number(fromNodeId || 0);
        const toId = Number(toNodeId || 0);
        if (!fromId || !toId || fromId === toId) return;

        const exists = workflowEdges.some(
            (edge) => Number(edge.from_node_id) === fromId && Number(edge.to_node_id) === toId
        );
        if (exists) return;

        setWorkflowLocal((prev) => ({
            ...prev,
            edges: [
                ...(prev.edges || []),
                {
                    id: makeTempId(),
                    workflow_id: prev.id,
                    from_node_id: fromId,
                    to_node_id: toId,
                },
            ],
        }));
    };

    const removeEdge = (edgeId) => {
        setWorkflowLocal((prev) => ({
            ...prev,
            edges: (prev.edges || []).filter((edge) => Number(edge.id) !== Number(edgeId)),
        }));
    };

    const handleCanvasDrop = (event) => {
        event.preventDefault();
        setCanvasDragOver(false);

        const pipelineId = event.dataTransfer.getData("text/plain");
        if (!pipelineId) return;

        const worldPoint = screenToWorld(event.clientX, event.clientY);
        const x = Math.max(10, Math.min(worldPoint.x - 110, GRAPH_SIZE - 230));
        const y = Math.max(10, Math.min(worldPoint.y - 32, GRAPH_SIZE - 80));

        addNodeFromPipeline(pipelineId, x, y);
    };

    const onDragStartPipeline = (event, pipelineId) => {
        event.dataTransfer.setData("text/plain", String(pipelineId));
        event.dataTransfer.effectAllowed = "copyMove";
    };

    const handleNodeClick = (nodeId) => {
        if (!linkMode) return;

        if (!linkSourceNodeId) {
            setLinkSourceNodeId(nodeId);
            return;
        }

        if (Number(linkSourceNodeId) === Number(nodeId)) {
            resetLinkState();
            return;
        }

        addEdge(linkSourceNodeId, nodeId);
        resetLinkState();
        setMessage("Dependency created. Save graph to persist.");
    };

    const handleMouseDownNode = (event, nodeId) => {
        if (linkMode) {
            handleNodeClick(nodeId);
            return;
        }
        const node = nodeById.get(Number(nodeId));
        if (!node) return;

        const worldPoint = screenToWorld(event.clientX, event.clientY);

        dragRef.current = {
            active: true,
            nodeId,
            offsetX: worldPoint.x - node.pos_x,
            offsetY: worldPoint.y - node.pos_y,
        };
    };

    const handleMouseMove = (event) => {
        if (!dragRef.current.active || !workflow) return;

        const worldPoint = screenToWorld(event.clientX, event.clientY);
        const x = worldPoint.x - dragRef.current.offsetX;
        const y = worldPoint.y - dragRef.current.offsetY;

        const nextX = Math.max(6, Math.min(x, GRAPH_SIZE - 230));
        const nextY = Math.max(6, Math.min(y, GRAPH_SIZE - 80));

        setWorkflowLocal((prev) => ({
            ...prev,
            nodes: (prev.nodes || []).map((node) =>
                Number(node.id) === Number(dragRef.current.nodeId)
                    ? { ...node, pos_x: nextX, pos_y: nextY }
                    : node
            ),
        }));
    };

    const handleMouseUp = () => {
        dragRef.current = { active: false, nodeId: null, offsetX: 0, offsetY: 0 };
    };

    const autoLayout = () => {
        if (!workflow) return;

        const cols = 3;
        setWorkflowLocal((prev) => ({
            ...prev,
            nodes: (prev.nodes || []).map((node, idx) => ({
                ...node,
                pos_x: 80 + (idx % cols) * 260,
                pos_y: 90 + Math.floor(idx / cols) * 140,
            })),
        }));

        setTimeout(() => fitGraphToCanvas(), 0);
        setMessage("Auto-layout applied. Save graph to persist.");
    };

    const saveGraph = async () => {
        if (!workflow) return;

        setSavingGraph(true);
        setError("");
        setMessage("");

        try {
            const payload = {
                nodes: (workflow.nodes || []).map((node) => ({
                    id: Number(node.id) > 0 ? Number(node.id) : null,
                    pipeline_id: Number(node.pipeline_id),
                    node_name: node.node_name || null,
                    pos_x: Number(node.pos_x || 100),
                    pos_y: Number(node.pos_y || 90),
                })),
                edges: (workflow.edges || []).map((edge) => ({
                    from_node_id: Number(edge.from_node_id),
                    to_node_id: Number(edge.to_node_id),
                })),
            };

            const updated = await updateWorkflowGraph(workflow.id, payload);
            setWorkflow({ ...updated, nodes: normalizeNodes(updated.nodes || []) });
            setMessage("Graph saved.");
            await loadWorkflows(workflow.id);
        } catch (err) {
            setError(err?.message || "Failed to save graph.");
        } finally {
            setSavingGraph(false);
        }
    };

    const zoomIn = () => {
        setView((prev) => ({ ...prev, zoom: clampZoom(prev.zoom + 0.1) }));
    };

    const zoomOut = () => {
        setView((prev) => ({ ...prev, zoom: clampZoom(prev.zoom - 0.1) }));
    };

    const resetView = () => {
        setView({ zoom: 1, offsetX: 0, offsetY: 0 });
    };

    const handleMiniMapClick = (event) => {
        const canvas = canvasRef.current;
        const miniMap = miniMapRef.current;
        if (!canvas || !miniMap) return;

        const rect = miniMap.getBoundingClientRect();
        const clickX = event.clientX - rect.left;
        const clickY = event.clientY - rect.top;

        const worldX = graphBounds.minX + (clickX - 8) / miniMapModel.scale;
        const worldY = graphBounds.minY + (clickY - 8) / miniMapModel.scale;

        const canvasWidth = canvas.clientWidth;
        const canvasHeight = canvas.clientHeight;

        setView((prev) => ({
            ...prev,
            offsetX: canvasWidth / 2 - worldX * prev.zoom,
            offsetY: canvasHeight / 2 - worldY * prev.zoom,
        }));
    };

    const runCurrentWorkflow = async () => {
        if (!workflow) return;

        setRunning(true);
        setError("");
        setMessage("");
        try {
            const run = await runWorkflow(workflow.id, {
                max_parallel_nodes: workflow.execution_mode === "parallel" ? 4 : 1,
                execution_mode: workflow.execution_mode || "serial",
                stop_on_error: workflow.stop_on_error !== false,
                allow_ddl_execute: false,
            });
            setSelectedRun(run);
            setMessage(`Run ${run.id} finished with ${run.status}.`);
            await loadWorkflowRuns(workflow.id);
            await loadWorkflowSchedule(workflow.id);
        } catch (err) {
            setError(err?.message || "Workflow execution failed.");
        } finally {
            setRunning(false);
        }
    };

    const openRunDetails = async (runId) => {
        setError("");
        try {
            const run = await fetchWorkflowRun(runId);
            setSelectedRun(run);
        } catch (err) {
            setError(err?.message || "Failed to fetch run details.");
        }
    };

    const saveSchedule = async () => {
        if (!workflow) return;

        setLoading(true);
        setError("");
        setMessage("");

        const payload = {
            schedule_type: scheduleForm.schedule_type,
            is_active: !!scheduleForm.is_active,
        };

        if (scheduleForm.schedule_type === "interval") {
            payload.interval_minutes = Number(scheduleForm.interval_minutes || 30);
        } else {
            payload.cron_expression = scheduleForm.cron_expression;
        }

        try {
            if (schedule?.id) {
                await updateWorkflowSchedule(workflow.id, payload);
                setMessage("Schedule updated.");
            } else {
                await createWorkflowSchedule(workflow.id, payload);
                setMessage("Schedule created.");
            }
            await loadWorkflowSchedule(workflow.id);
        } catch (err) {
            setError(err?.message || "Failed to save schedule.");
        } finally {
            setLoading(false);
        }
    };

    const clearSchedule = async () => {
        if (!workflow || !schedule?.id) return;

        setLoading(true);
        setError("");
        setMessage("");

        try {
            await deleteWorkflowSchedule(workflow.id);
            setSchedule(null);
            setScheduleForm(DEFAULT_SCHEDULE_FORM);
            setMessage("Schedule removed.");
        } catch (err) {
            setError(err?.message || "Failed to remove schedule.");
        } finally {
            setLoading(false);
        }
    };

    if (!hasConnectionContext) {
        return (
            <div className="pipeline-main-card envtools-card">
                <div className="pipeline-empty">
                    Save and select a PostgreSQL connection in Explorer first, then reopen this tab.
                </div>
            </div>
        );
    }

    return (
        <div className="workflow-studio-page">
            <div className="module-hero compact-hero workflow-studio-hero">
                <div className="module-badge">Engineering</div>
                <h1 className="module-title">Workflow Studio</h1>
                <p className="module-subtitle">
                    Drag pipelines from the left palette into the canvas, connect dependencies, and run one complete workflow job.
                </p>
            </div>

            {error ? <div className="pipeline-inline-banner error">{error}</div> : null}
            {message ? <div className="pipeline-inline-banner success">{message}</div> : null}

            <section className="workflow-studio-toolbar">
                <div className="workflow-studio-toolbar-group">
                    <label>Workflow</label>
                    <select
                        value={selectedWorkflowId || ""}
                        onChange={(e) => {
                            const id = Number(e.target.value || 0) || null;
                            setSelectedWorkflowId(id);
                            loadWorkflows(id);
                        }}
                    >
                        <option value="">Select workflow</option>
                        {workflows.map((item) => (
                            <option key={item.id} value={item.id}>
                                {item.name}
                            </option>
                        ))}
                    </select>
                </div>

                <div className="workflow-studio-toolbar-group grow">
                    <label>New workflow</label>
                    <input
                        value={newWorkflowName}
                        onChange={(e) => setNewWorkflowName(e.target.value)}
                        placeholder="e.g. src_to_core_to_rpt_daily"
                    />
                </div>

                <button type="button" className="pipeline-primary-btn" onClick={handleCreateWorkflow} disabled={loading}>
                    Create
                </button>

                <button type="button" className="pipeline-secondary-btn" onClick={() => refreshAll(selectedWorkflowId)} disabled={loading}>
                    Refresh
                </button>
            </section>

            <section className="workflow-studio-layout">
                <aside className="workflow-studio-palette">
                    <div className="workflow-studio-panel-header">
                        <h3>Pipeline Palette</h3>
                        <span>{filteredPipelines.length}</span>
                    </div>
                    <input
                        className="workflow-studio-search"
                        value={pipelineSearch}
                        onChange={(e) => setPipelineSearch(e.target.value)}
                        placeholder="Search pipelines"
                    />

                    <div className="workflow-studio-pipeline-list">
                        {pipelineTree.map((db) => (
                            <details key={db.dbName} className="workflow-studio-tree-level workflow-studio-tree-db" open>
                                <summary>
                                    <span>{db.dbName}</span>
                                    <small>{db.count}</small>
                                </summary>

                                {db.schemas.map((schema) => (
                                    <details key={`${db.dbName}-${schema.schemaName}`} className="workflow-studio-tree-level workflow-studio-tree-schema" open>
                                        <summary>
                                            <span>{schema.schemaName}</span>
                                            <small>{schema.count}</small>
                                        </summary>

                                        {schema.objects.map((objectNode) => (
                                            <details
                                                key={`${db.dbName}-${schema.schemaName}-${objectNode.objectName}`}
                                                className="workflow-studio-tree-level workflow-studio-tree-object"
                                                open={pipelineSearch.trim().length > 0}
                                            >
                                                <summary>
                                                    <span>{objectNode.objectName}</span>
                                                    <small>{objectNode.items.length}</small>
                                                </summary>

                                                <div className="workflow-studio-tree-pipeline-items">
                                                    {objectNode.items.map((pipeline) => (
                                                        <div
                                                            key={pipeline.id}
                                                            className="workflow-studio-pipeline-card"
                                                            draggable
                                                            onDragStart={(event) => onDragStartPipeline(event, pipeline.id)}
                                                            onDoubleClick={() => addNodeFromPipeline(pipeline.id)}
                                                            title="Drag to canvas or double-click to add"
                                                        >
                                                            <strong>{pipeline.name || `Pipeline ${pipeline.id}`}</strong>
                                                            <span>#{pipeline.id}</span>
                                                            <small>
                                                                {(pipeline.source_object || "src")}
                                                                {" -> "}
                                                                {(pipeline.target_object || "target")}
                                                            </small>
                                                            <small className="workflow-studio-flow-chip">{getPipelineFlowGroup(pipeline)}</small>
                                                        </div>
                                                    ))}
                                                </div>
                                            </details>
                                        ))}
                                    </details>
                                ))}
                            </details>
                        ))}
                        {!filteredPipelines.length ? (
                            <div className="pipeline-empty">No pipelines found. Create one in ETL / Lineage Pipeline Builder.</div>
                        ) : null}
                    </div>

                    <div className="workflow-studio-hint">
                        1) Drag pipelines into canvas
                        <br />
                        2) Enable Link mode
                        <br />
                        3) Click source node then target node
                        <br />
                        4) Save graph and Run workflow
                    </div>
                </aside>

                <main className="workflow-studio-canvas-panel">
                    {!workflow ? (
                        <div className="pipeline-empty">Create or select a workflow to start building the graph.</div>
                    ) : (
                        <>
                            <div className="workflow-studio-canvas-toolbar">
                                <div className="workflow-studio-canvas-toolbar-left">
                                    <input
                                        value={workflow.name || ""}
                                        onChange={(e) => setWorkflowLocal({ name: e.target.value })}
                                        placeholder="Workflow name"
                                    />
                                    <select
                                        value={workflow.execution_mode || "serial"}
                                        onChange={(e) => setWorkflowLocal({ execution_mode: e.target.value })}
                                    >
                                        <option value="serial">Serial</option>
                                        <option value="parallel">Parallel</option>
                                    </select>
                                    <label className="workflow-studio-inline-check">
                                        <input
                                            type="checkbox"
                                            checked={workflow.stop_on_error !== false}
                                            onChange={(e) => setWorkflowLocal({ stop_on_error: e.target.checked })}
                                        />
                                        Stop on error
                                    </label>
                                </div>

                                <div className="workflow-studio-canvas-toolbar-right">
                                    <button
                                        type="button"
                                        className={`pipeline-secondary-btn ${linkMode ? "active" : ""}`}
                                        onClick={() => {
                                            setLinkMode((prev) => !prev);
                                            resetLinkState();
                                        }}
                                    >
                                        {linkMode ? "Link Mode On" : "Link Mode"}
                                    </button>
                                    <button type="button" className="pipeline-secondary-btn" onClick={autoLayout}>
                                        Auto Layout
                                    </button>
                                    <div className="workflow-studio-zoom-controls">
                                        <button type="button" className="pipeline-secondary-btn" onClick={zoomOut}>
                                            -
                                        </button>
                                        <span>{Math.round(view.zoom * 100)}%</span>
                                        <button type="button" className="pipeline-secondary-btn" onClick={zoomIn}>
                                            +
                                        </button>
                                        <button type="button" className="pipeline-secondary-btn" onClick={fitGraphToCanvas}>
                                            Fit
                                        </button>
                                        <button type="button" className="pipeline-secondary-btn" onClick={resetView}>
                                            Reset
                                        </button>
                                    </div>
                                    <button type="button" className="pipeline-primary-btn" onClick={saveGraph} disabled={savingGraph}>
                                        {savingGraph ? "Saving..." : "Save Graph"}
                                    </button>
                                    <button type="button" className="pipeline-primary-btn" onClick={runCurrentWorkflow} disabled={running}>
                                        {running ? "Running..." : "Run"}
                                    </button>
                                </div>
                            </div>

                            <div
                                ref={canvasRef}
                                className={`workflow-studio-canvas ${canvasDragOver ? "drag-over" : ""}`}
                                onDragOver={(event) => {
                                    event.preventDefault();
                                    setCanvasDragOver(true);
                                }}
                                onDragLeave={() => setCanvasDragOver(false)}
                                onDrop={handleCanvasDrop}
                                onMouseMove={handleMouseMove}
                                onMouseUp={handleMouseUp}
                                onMouseLeave={handleMouseUp}
                            >
                                <div
                                    className="workflow-studio-graph-surface"
                                    style={{ transform: `translate(${view.offsetX}px, ${view.offsetY}px) scale(${view.zoom})` }}
                                >
                                    <svg className="workflow-studio-edges" width={GRAPH_SIZE} height={GRAPH_SIZE}>
                                        <defs>
                                            <marker
                                                id="workflow-studio-arrow"
                                                markerWidth="10"
                                                markerHeight="8"
                                                refX="9"
                                                refY="4"
                                                orient="auto"
                                                markerUnits="strokeWidth"
                                            >
                                                <path d="M0,0 L10,4 L0,8 z" className="workflow-studio-edge-arrow" />
                                            </marker>
                                        </defs>
                                        {workflowEdges.map((edge) => {
                                            const fromNode = nodeById.get(Number(edge.from_node_id));
                                            const toNode = nodeById.get(Number(edge.to_node_id));
                                            if (!fromNode || !toNode) return null;

                                            const x1 = fromNode.pos_x + 115;
                                            const y1 = fromNode.pos_y + 26;
                                            const x2 = toNode.pos_x;
                                            const y2 = toNode.pos_y + 26;

                                            return (
                                                <line
                                                    key={`${edge.id}-${edge.from_node_id}-${edge.to_node_id}`}
                                                    x1={x1}
                                                    y1={y1}
                                                    x2={x2}
                                                    y2={y2}
                                                    className="workflow-studio-edge-line"
                                                    markerEnd="url(#workflow-studio-arrow)"
                                                    onClick={() => removeEdge(edge.id)}
                                                />
                                            );
                                        })}
                                    </svg>

                                    {workflowNodes.map((node) => {
                                        const isLinkSource = Number(linkSourceNodeId) === Number(node.id);
                                        return (
                                            <div
                                                key={node.id}
                                                className={`workflow-studio-node ${isLinkSource ? "link-source" : ""}`}
                                                style={{ left: `${node.pos_x}px`, top: `${node.pos_y}px` }}
                                                onMouseDown={(event) => handleMouseDownNode(event, node.id)}
                                                title={linkMode ? "Click node to create dependency" : "Drag to reposition"}
                                            >
                                                <div className="workflow-studio-node-title">{nodeLabel(node)}</div>
                                                <div className="workflow-studio-node-subtitle">Pipeline #{node.pipeline_id}</div>
                                                <button
                                                    type="button"
                                                    className="workflow-studio-node-remove"
                                                    onClick={(event) => {
                                                        event.stopPropagation();
                                                        removeNode(node.id);
                                                    }}
                                                >
                                                    x
                                                </button>
                                            </div>
                                        );
                                    })}
                                </div>

                                {!workflowNodes.length ? (
                                    <div className="workflow-studio-empty-canvas">
                                        Drag pipelines here to start building the workflow graph.
                                    </div>
                                ) : null}
                            </div>

                            <div className="workflow-studio-footer-row">
                                <div className="workflow-studio-subpanel">
                                    <h4>Dependencies</h4>
                                    {!workflowEdges.length ? (
                                        <div className="pipeline-empty">No dependencies yet.</div>
                                    ) : (
                                        <div className="workflow-studio-edge-list">
                                            {workflowEdges.map((edge) => {
                                                const fromNode = nodeById.get(Number(edge.from_node_id));
                                                const toNode = nodeById.get(Number(edge.to_node_id));
                                                return (
                                                    <div key={edge.id} className="workflow-studio-edge-row">
                                                        <span>
                                                            {fromNode ? nodeLabel(fromNode) : edge.from_node_id}
                                                            {" -> "}
                                                            {toNode ? nodeLabel(toNode) : edge.to_node_id}
                                                        </span>
                                                        <button type="button" className="pipeline-secondary-btn" onClick={() => removeEdge(edge.id)}>
                                                            Remove
                                                        </button>
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    )}
                                </div>

                                <div className="workflow-studio-subpanel">
                                    <h4>Automation</h4>
                                    <div className="workflow-studio-inline-form">
                                        <select
                                            value={scheduleForm.schedule_type}
                                            onChange={(e) => setScheduleForm((prev) => ({ ...prev, schedule_type: e.target.value }))}
                                        >
                                            <option value="interval">Interval</option>
                                            <option value="cron">Cron</option>
                                        </select>

                                        {scheduleForm.schedule_type === "interval" ? (
                                            <input
                                                type="number"
                                                min={1}
                                                value={scheduleForm.interval_minutes}
                                                onChange={(e) => setScheduleForm((prev) => ({ ...prev, interval_minutes: e.target.value }))}
                                                placeholder="Minutes"
                                            />
                                        ) : (
                                            <input
                                                value={scheduleForm.cron_expression}
                                                onChange={(e) => setScheduleForm((prev) => ({ ...prev, cron_expression: e.target.value }))}
                                                placeholder="0 * * * *"
                                            />
                                        )}

                                        <label className="workflow-studio-inline-check">
                                            <input
                                                type="checkbox"
                                                checked={scheduleForm.is_active}
                                                onChange={(e) => setScheduleForm((prev) => ({ ...prev, is_active: e.target.checked }))}
                                            />
                                            Active
                                        </label>
                                    </div>

                                    <div className="workflow-studio-inline-buttons">
                                        <button type="button" className="pipeline-primary-btn" onClick={saveSchedule} disabled={loading}>
                                            {schedule?.id ? "Update Schedule" : "Create Schedule"}
                                        </button>
                                        <button
                                            type="button"
                                            className="pipeline-danger-btn"
                                            onClick={clearSchedule}
                                            disabled={!schedule?.id || loading}
                                        >
                                            Remove
                                        </button>
                                    </div>
                                    <div className="workflow-studio-caption">
                                        Last run: {schedule?.last_run_at || "-"} | Next run: {schedule?.next_run_at || "-"}
                                    </div>
                                </div>

                                <div className="workflow-studio-subpanel">
                                    <h4>Recent Runs</h4>
                                    {!workflowRuns.length ? (
                                        <div className="pipeline-empty">No runs yet.</div>
                                    ) : (
                                        <div className="workflow-studio-run-list">
                                            {workflowRuns.slice(0, 6).map((run) => (
                                                <button
                                                    key={run.id}
                                                    type="button"
                                                    className="workflow-studio-run-row"
                                                    onClick={() => openRunDetails(run.id)}
                                                >
                                                    <strong>#{run.id}</strong>
                                                    <span>{run.status}</span>
                                                    <small>s:{run.success_nodes} f:{run.failed_nodes} sk:{run.skipped_nodes}</small>
                                                </button>
                                            ))}
                                        </div>
                                    )}

                                    {selectedRun ? (
                                        <div className="workflow-studio-run-summary">
                                            Selected run #{selectedRun.id}: {selectedRun.status}
                                        </div>
                                    ) : null}
                                </div>

                                <div className="workflow-studio-subpanel">
                                    <h4>Mini Map</h4>
                                    <svg
                                        ref={miniMapRef}
                                        className="workflow-studio-minimap"
                                        width={miniMapModel.mapWidth}
                                        height={miniMapModel.mapHeight}
                                        onClick={handleMiniMapClick}
                                    >
                                        {workflowEdges.map((edge) => {
                                            const fromNode = nodeById.get(Number(edge.from_node_id));
                                            const toNode = nodeById.get(Number(edge.to_node_id));
                                            if (!fromNode || !toNode) return null;

                                            return (
                                                <line
                                                    key={`mini-${edge.id}`}
                                                    x1={miniMapModel.projectX(fromNode.pos_x + NODE_WIDTH / 2)}
                                                    y1={miniMapModel.projectY(fromNode.pos_y + NODE_HEIGHT / 2)}
                                                    x2={miniMapModel.projectX(toNode.pos_x + NODE_WIDTH / 2)}
                                                    y2={miniMapModel.projectY(toNode.pos_y + NODE_HEIGHT / 2)}
                                                    className="workflow-studio-minimap-edge"
                                                />
                                            );
                                        })}

                                        {workflowNodes.map((node) => (
                                            <rect
                                                key={`mini-node-${node.id}`}
                                                x={miniMapModel.projectX(node.pos_x)}
                                                y={miniMapModel.projectY(node.pos_y)}
                                                width={Math.max(4, NODE_WIDTH * miniMapModel.scale)}
                                                height={Math.max(3, NODE_HEIGHT * miniMapModel.scale)}
                                                className="workflow-studio-minimap-node"
                                            />
                                        ))}

                                        {(() => {
                                            const viewport = getWorldViewport();
                                            if (!viewport) return null;
                                            return (
                                                <rect
                                                    x={miniMapModel.projectX(viewport.x)}
                                                    y={miniMapModel.projectY(viewport.y)}
                                                    width={Math.max(10, viewport.width * miniMapModel.scale)}
                                                    height={Math.max(8, viewport.height * miniMapModel.scale)}
                                                    className="workflow-studio-minimap-viewport"
                                                />
                                            );
                                        })()}
                                    </svg>
                                    <div className="workflow-studio-caption">Click mini map to center canvas.</div>
                                </div>
                            </div>

                            <div className="workflow-studio-actions-row">
                                <button type="button" className="pipeline-secondary-btn" onClick={handleSaveWorkflowMeta} disabled={loading}>
                                    Save Workflow Settings
                                </button>
                                <button type="button" className="pipeline-danger-btn" onClick={handleDeleteWorkflow} disabled={loading}>
                                    Delete Workflow
                                </button>
                            </div>
                        </>
                    )}
                </main>
            </section>
        </div>
    );
}
