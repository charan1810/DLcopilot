import { getStoredToken } from "./schemaApi";

const API_BASE = (import.meta.env.VITE_API_BASE_URL || "http://localhost:8000").replace(/\/$/, "");

function authHeaders(contentType = true) {
    const token = getStoredToken();
    const headers = {};
    if (contentType) headers["Content-Type"] = "application/json";
    if (token) headers.Authorization = `Bearer ${token}`;
    return headers;
}

async function requestJson(path, method = "GET", body = null) {
    const res = await fetch(`${API_BASE}${path}`, {
        method,
        headers: authHeaders(true),
        body: body ? JSON.stringify(body) : undefined,
    });

    let data = null;
    try {
        data = await res.json();
    } catch {
        data = null;
    }

    if (!res.ok) {
        throw new Error(data?.detail || data?.error || data?.message || "Request failed");
    }

    return { data };
}

async function requestBlob(path, method = "POST", body = null) {
    const res = await fetch(`${API_BASE}${path}`, {
        method,
        headers: authHeaders(body ? true : false),
        body: body ? JSON.stringify(body) : undefined,
    });

    if (!res.ok) {
        let msg = "Request failed";
        try {
            const errJson = await res.json();
            msg = errJson?.detail || errJson?.error || msg;
        } catch {
            // ignore
        }
        throw new Error(msg);
    }

    const blob = await res.blob();
    return { data: blob, headers: res.headers };
}

export async function analyzeDiffs(items) {
    return requestJson("/api/diff/analyze", "POST", { items });
}

export async function syncSchemaZip(payload) {
    return requestBlob("/api/sync/schema", "POST", payload);
}

export async function queryColumns(payload) {
    return requestJson("/api/query-columns", "POST", payload);
}

export async function optimizeQuery(payload) {
    return requestJson("/api/optimize-query", "POST", payload);
}

export async function inferPrimaryKeys(payload) {
    return requestJson("/api/infer-primary-keys", "POST", payload);
}

export async function dedupAdvanced(payload) {
    return requestJson("/api/dedup-advanced", "POST", payload);
}

export async function benchmarkView(payload) {
    return requestJson("/api/benchmark-view", "POST", payload);
}
