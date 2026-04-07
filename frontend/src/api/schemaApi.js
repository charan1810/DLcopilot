const API_BASE = "http://localhost:8000";

// ── Auth token helpers ───────────────────────────────────────
export function getStoredToken() {
    return localStorage.getItem("dc_token");
}

export function setStoredToken(token) {
    localStorage.setItem("dc_token", token);
}

export function clearStoredToken() {
    localStorage.removeItem("dc_token");
}

function authHeaders() {
    const token = getStoredToken();
    const h = { "Content-Type": "application/json" };
    if (token) h["Authorization"] = `Bearer ${token}`;
    return h;
}

function authHeadersGet() {
    const token = getStoredToken();
    if (!token) return {};
    return { Authorization: `Bearer ${token}` };
}

async function parseJson(res) {
    let data = null;

    try {
        data = await res.json();
    } catch {
        data = null;
    }

    if (!res.ok) {
        throw new Error(data?.detail || data?.message || "Request failed");
    }

    return data;
}

// ── Auth APIs ────────────────────────────────────────────────
export async function signup(payload) {
    const res = await fetch(`${API_BASE}/api/auth/signup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function login(payload) {
    const res = await fetch(`${API_BASE}/api/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function fetchCurrentUser() {
    const res = await fetch(`${API_BASE}/api/auth/me`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

// ── Admin APIs ───────────────────────────────────────────────
export async function fetchUsers() {
    const res = await fetch(`${API_BASE}/api/admin/users`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function updateUser(userId, payload) {
    const res = await fetch(`${API_BASE}/api/admin/users/${userId}`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function deleteUser(userId) {
    const res = await fetch(`${API_BASE}/api/admin/users/${userId}`, {
        method: "DELETE",
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function testConnection(payload) {
    const res = await fetch(`${API_BASE}/api/connections/test`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function saveConnection(payload) {
    const res = await fetch(`${API_BASE}/api/connections/save`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function fetchSavedConnections() {
    const res = await fetch(`${API_BASE}/api/connections`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function fetchDatabases(connectionId) {
    const res = await fetch(`${API_BASE}/api/schema-explorer/databases?connection_id=${connectionId}`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function fetchSchemas(connectionId, databaseName) {
    const res = await fetch(
        `${API_BASE}/api/schema-explorer/schemas?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}`,
        { headers: authHeadersGet() }
    );
    return parseJson(res);
}

export async function fetchObjects(connectionId, databaseName, schema) {
    const res = await fetch(
        `${API_BASE}/api/schema-explorer/objects?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}`,
        { headers: authHeadersGet() }
    );
    return parseJson(res);
}

export async function fetchObjectDetails(connectionId, databaseName, schema, objectName) {
    const res = await fetch(
        `${API_BASE}/api/schema-explorer/object-details?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}&object=${encodeURIComponent(objectName)}`,
        { headers: authHeadersGet() }
    );
    return parseJson(res);
}

export async function fetchObjectSampleData(connectionId, databaseName, schema, objectName, limit = 25) {
    const res = await fetch(
        `${API_BASE}/api/schema-explorer/sample-data?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}&object=${encodeURIComponent(objectName)}&limit=${limit}`,
        { headers: authHeadersGet() }
    );
    return parseJson(res);
}

export async function fetchRelationships(connectionId, databaseName, schema, objectName) {
    const res = await fetch(
        `${API_BASE}/api/connections/${connectionId}/relationships?database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}&object=${encodeURIComponent(objectName)}`,
        { headers: authHeadersGet() }
    );
    return parseJson(res);
}

export async function fetchDefinition(connectionId, databaseName, schema, objectName) {
    const res = await fetch(
        `${API_BASE}/api/connections/${connectionId}/definition?database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}&object=${encodeURIComponent(objectName)}`,
        { headers: authHeadersGet() }
    );
    return parseJson(res);
}

export async function executeQuery(connectionId, databaseName, payload) {
    const res = await fetch(
        `${API_BASE}/api/query/execute?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}`,
        {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify(payload),
        }
    );
    return parseJson(res);
}

export async function fetchLineage(connectionId, databaseName, schema, objectName) {
    const res = await fetch(
        `${API_BASE}/api/lineage?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}&object=${encodeURIComponent(objectName)}`,
        { headers: authHeadersGet() }
    );
    return parseJson(res);
}

export async function fetchAiJoinSuggestions(connectionId, databaseName, schema, objectName) {
    const res = await fetch(`${API_BASE}/api/ai/join-suggestions`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            connection_id: connectionId,
            database_name: databaseName,
            schema,
            object_name: objectName,
        }),
    });
    return parseJson(res);
}

export async function generateAiSql(
    connectionId,
    databaseName,
    schema,
    objectName,
    userPrompt,
    selectedTables = []
) {
    const res = await fetch(`${API_BASE}/api/ai/generate-sql`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            connection_id: connectionId,
            database_name: databaseName,
            schema,
            object_name: objectName,
            user_prompt: userPrompt,
            selected_tables: selectedTables,
        }),
    });
    return parseJson(res);
}

export async function resolveObjects(connectionId, databaseName, schema, userPrompt) {
    const res = await fetch(`${API_BASE}/api/ai/resolve-objects`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            connection_id: connectionId,
            database_name: databaseName,
            schema,
            user_prompt: userPrompt,
        }),
    });
    return parseJson(res);
}

export async function runAgenticInsights(payload) {
    const res = await fetch(`${API_BASE}/api/insights/agentic-rag`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function getAITransformationSuggestions(payload) {
    const response = await fetch(`${API_BASE}/api/ai/transformation-suggestions`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });

    return parseJson(response);
}

export async function fixSqlQuery(connectionId, databaseName, sql, error, context = {}) {
    const response = await fetch(`${API_BASE}/api/fix-sql`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            connection_id: connectionId,
            database_name: databaseName,
            schema: context.schema || "public",
            object_name: context.object_name || "",
            sql,
            error,
        }),
    });

    return parseJson(response);
}

export async function saveTransformationRecipe(payload) {
    const response = await fetch(`${API_BASE}/api/recipes`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });

    return parseJson(response);
}

export async function fetchTransformationRecipes(connectionId, databaseName, schema, objectName) {
    const response = await fetch(
        `${API_BASE}/api/recipes?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}&object_name=${encodeURIComponent(objectName)}`,
        { headers: authHeadersGet() }
    );
    return parseJson(response);
}

export async function deleteTransformationRecipe(recipeId) {
    const response = await fetch(`${API_BASE}/api/recipes/${recipeId}`, {
        method: "DELETE",
        headers: authHeadersGet(),
    });

    return parseJson(response);
}

export async function fetchPromptHistory(connectionId, databaseName, schema, objectName, limit = 10) {
    const response = await fetch(
        `${API_BASE}/api/prompt-history?connection_id=${connectionId}&database_name=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schema)}&object_name=${encodeURIComponent(objectName)}&limit=${limit}`,
        { headers: authHeadersGet() }
    );
    return parseJson(response);
}

export async function generateDataQualitySql(connectionId, databaseName, schema, objectName) {
    const response = await fetch(`${API_BASE}/api/ai/data-quality-sql`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            connection_id: connectionId,
            database_name: databaseName,
            schema,
            object_name: objectName,
        }),
    });

    return parseJson(response);
}
// ============================================================
// PIPELINE BUILDER APIs
// ============================================================

export async function fetchPipelines(params = {}) {
    const search = new URLSearchParams();
    if (params.connection_id) search.append("connection_id", params.connection_id);
    if (params.database_name) search.append("database_name", params.database_name);
    if (params.schema_name) search.append("schema_name", params.schema_name);

    const res = await fetch(`${API_BASE}/api/pipelines?${search.toString()}`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function createPipeline(payload) {
    const res = await fetch(`${API_BASE}/api/pipelines`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function getPipeline(pipelineId) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function updatePipeline(pipelineId, payload) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function deletePipeline(pipelineId) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}`, {
        method: "DELETE",
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function addPipelineStep(pipelineId, payload) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/steps`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function importPipelineSteps(pipelineId, payload) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/steps/import`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function removePipelineStep(pipelineId, stepId) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/steps/${stepId}`, {
        method: "DELETE",
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function updatePipelineStep(pipelineId, stepId, payload) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/steps/${stepId}`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function executePipeline(pipelineId, payload = { stop_on_error: true }) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/execute`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function fetchPipelineRuns(pipelineId) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/runs`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function fetchAllPipelineRuns(limit = 50) {
    const res = await fetch(`${API_BASE}/api/pipelines/runs?limit=${limit}`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function fetchPipelineRun(runId) {
    const res = await fetch(`${API_BASE}/api/pipeline-runs/${runId}`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function runPipeline(pipelineId, payload = {}) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/run`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            stop_on_error: true,
            trigger_type: "MANUAL",
            ...payload,
        }),
    });
    return parseJson(res);
}

export async function retryPipelineRun(runId, payload = {}) {
    const res = await fetch(`${API_BASE}/api/pipeline-runs/${runId}/retry`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

// Schedule APIs
export async function fetchPipelineSchedule(pipelineId) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/schedule`, {
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function createPipelineSchedule(pipelineId, payload) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/schedule`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function updatePipelineSchedule(pipelineId, payload) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/schedule`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function deletePipelineSchedule(pipelineId) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/schedule`, {
        method: "DELETE",
        headers: authHeadersGet(),
    });
    return parseJson(res);
}

export async function agenticGeneratePipelineSteps(pipelineId, userRequirement, selectedTables = []) {
    const res = await fetch(`${API_BASE}/api/pipelines/${pipelineId}/ai-generate-steps`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            user_requirement: userRequirement,
            selected_tables: selectedTables,
        }),
    });
    return parseJson(res);
}