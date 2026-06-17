import { getStoredToken } from "./schemaApi";

const API_BASE = (import.meta.env.VITE_API_BASE_URL || "http://localhost:8000").replace(/\/$/, "");

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
    if (!res.ok) throw new Error(data?.detail || data?.message || "Request failed");
    return data;
}

export async function listModels() {
    const res = await fetch(`${API_BASE}/api/models`, { headers: authHeadersGet() });
    return parseJson(res);
}

export async function getModel(modelId) {
    const res = await fetch(`${API_BASE}/api/models/${modelId}`, { headers: authHeadersGet() });
    return parseJson(res);
}

export async function createModel(payload) {
    const res = await fetch(`${API_BASE}/api/models`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function updateModel(modelId, payload) {
    const res = await fetch(`${API_BASE}/api/models/${modelId}`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function deleteModel(modelId) {
    const res = await fetch(`${API_BASE}/api/models/${modelId}`, {
        method: "DELETE",
        headers: authHeadersGet(),
    });
    if (!res.ok) {
        let data = null;
        try { data = await res.json(); } catch { /* ignore */ }
        throw new Error(data?.detail || "Delete failed");
    }
}

export async function validateModel(modelId, businessRules) {
    const res = await fetch(`${API_BASE}/api/models/${modelId}/validate`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ business_rules: businessRules }),
    });
    return parseJson(res);
}

export async function deployModel(modelId, targetConnectionId, targetSchema) {
    const res = await fetch(`${API_BASE}/api/models/${modelId}/deploy`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ target_connection_id: targetConnectionId, target_schema: targetSchema }),
    });
    return parseJson(res);
}

export async function createSchema(connectionId, schemaName, databaseName = "", password = "") {
    const res = await fetch(`${API_BASE}/api/metadata/create-schema`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
            connection_id: connectionId,
            schema_name: schemaName,
            database_name: databaseName,
            password,
        }),
    });
    return parseJson(res);
}

export async function listModelTemplates() {
    const res = await fetch(`${API_BASE}/api/models/templates`, { headers: authHeadersGet() });
    return parseJson(res);
}

export async function createModelTemplate(payload) {
    const res = await fetch(`${API_BASE}/api/models/templates`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}

export async function createModelsFromSchema(payload) {
    const res = await fetch(`${API_BASE}/api/models/bulk-from-schema`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
    });
    return parseJson(res);
}
