import { useEffect, useState, useCallback } from "react";
import {
    fetchUsers, updateUser, deleteUser,
    adminListConnections, adminCreateConnection, adminDeleteConnection, adminAssignConnection,
} from "../api/schemaApi";

const ROLE_OPTIONS = ["admin", "architect", "developer", "tester"];

const DB_TYPE_OPTIONS = ["postgres", "mysql", "snowflake"];

const BLANK_CONN_FORM = {
    name: "", db_type: "postgres", host: "localhost", port: "5432",
    database_name: "", schema_name: "src", username: "", password: "",
    account: "", warehouse: "", role: "",
};

// ── Connections Tab ───────────────────────────────────────────────────────────

function ConnectionsTab() {
    const [connections, setConnections] = useState([]);
    const [users, setUsers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [success, setSuccess] = useState("");
    const [showForm, setShowForm] = useState(false);
    const [form, setForm] = useState(BLANK_CONN_FORM);
    const [saving, setSaving] = useState(false);

    const load = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const [c, u] = await Promise.all([adminListConnections(), fetchUsers()]);
            setConnections(c || []);
            setUsers(u || []);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => { load(); }, [load]);

    const handleCreate = async () => {
        if (!form.name.trim()) { setError("Connection name is required."); return; }
        setSaving(true); setError(""); setSuccess("");
        try {
            await adminCreateConnection(form);
            setSuccess(`Connection "${form.name}" created.`);
            setForm(BLANK_CONN_FORM);
            setShowForm(false);
            load();
        } catch (err) {
            setError(err.message);
        } finally { setSaving(false); }
    };

    const handleDelete = async (connId, name) => {
        if (!confirm(`Delete connection "${name}"? It will be unassigned from all users.`)) return;
        setError(""); setSuccess("");
        try {
            await adminDeleteConnection(connId);
            setSuccess(`Connection "${name}" deleted.`);
            load();
        } catch (err) { setError(err.message); }
    };

    const handleAssign = async (userId, connectionId) => {
        setError(""); setSuccess("");
        try {
            const connId = connectionId === "" ? null : parseInt(connectionId, 10);
            const updated = await adminAssignConnection(userId, connId);
            const connName = connections.find(c => c.id === connId)?.name || "None";
            setSuccess(`Connection "${connName}" assigned to ${updated.full_name}.`);
            load();
        } catch (err) { setError(err.message); }
    };

    const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

    return (
        <div>
            {error && <div className="auth-error" style={{ marginBottom: 12 }}>{error}</div>}
            {success && (
                <div style={{ marginBottom: 12, padding: "10px 14px", background: "var(--success-bg)", border: "1px solid var(--success-border)", borderRadius: 10, color: "var(--success-text)", fontSize: 13 }}>
                    {success}
                </div>
            )}

            {/* ── Connections list ── */}
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
                <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700 }}>Saved Connections ({connections.length})</h3>
                <button className="btn-sm btn-primary" onClick={() => setShowForm(v => !v)}>
                    {showForm ? "Cancel" : "+ New Connection"}
                </button>
            </div>

            {showForm && (
                <div style={{ background: "var(--bg-soft-2)", border: "1px solid var(--border)", borderRadius: 12, padding: 20, marginBottom: 20, display: "flex", flexDirection: "column", gap: 12 }}>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                            <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Connection Name *</label>
                            <input className="auth-input" placeholder="e.g. Production Snowflake" value={form.name} onChange={e => set("name", e.target.value)} />
                        </div>
                        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                            <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>DB Type</label>
                            <select className="auth-input admin-select" value={form.db_type} onChange={e => set("db_type", e.target.value)}>
                                {DB_TYPE_OPTIONS.map(t => <option key={t}>{t}</option>)}
                            </select>
                        </div>
                        {form.db_type !== "snowflake" ? (
                            <>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Host</label>
                                    <input className="auth-input" placeholder="localhost" value={form.host} onChange={e => set("host", e.target.value)} />
                                </div>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Port</label>
                                    <input className="auth-input" placeholder="5432" value={form.port} onChange={e => set("port", e.target.value)} />
                                </div>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Database</label>
                                    <input className="auth-input" value={form.database_name} onChange={e => set("database_name", e.target.value)} />
                                </div>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Schema</label>
                                    <input className="auth-input" value={form.schema_name} onChange={e => set("schema_name", e.target.value)} />
                                </div>
                            </>
                        ) : (
                            <>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Account</label>
                                    <input className="auth-input" value={form.account} onChange={e => set("account", e.target.value)} />
                                </div>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Warehouse</label>
                                    <input className="auth-input" value={form.warehouse} onChange={e => set("warehouse", e.target.value)} />
                                </div>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Database</label>
                                    <input className="auth-input" value={form.database_name} onChange={e => set("database_name", e.target.value)} />
                                </div>
                                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                    <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Role</label>
                                    <input className="auth-input" value={form.role} onChange={e => set("role", e.target.value)} />
                                </div>
                            </>
                        )}
                        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                            <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Username</label>
                            <input className="auth-input" value={form.username} onChange={e => set("username", e.target.value)} />
                        </div>
                        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                            <label style={{ fontSize: 11, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase" }}>Password</label>
                            <input className="auth-input" type="password" value={form.password} onChange={e => set("password", e.target.value)} />
                        </div>
                    </div>
                    <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
                        <button className="btn-sm btn-outline" onClick={() => setShowForm(false)}>Cancel</button>
                        <button className="btn-sm btn-primary" onClick={handleCreate} disabled={saving}>
                            {saving ? "Saving…" : "Save Connection"}
                        </button>
                    </div>
                </div>
            )}

            {loading ? (
                <p style={{ opacity: 0.6, padding: 12 }}>Loading…</p>
            ) : connections.length === 0 ? (
                <p style={{ opacity: 0.6, padding: 12 }}>No connections yet. Create one above.</p>
            ) : (
                <div className="admin-table-wrap">
                    <table className="admin-table">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Name</th>
                                <th>Type</th>
                                <th>Host / Account</th>
                                <th>Database</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {connections.map(c => (
                                <tr key={c.id}>
                                    <td>{c.id}</td>
                                    <td style={{ fontWeight: 600 }}>{c.name}</td>
                                    <td><span className="role-badge" style={{ background: "var(--bg-soft)", color: "var(--text-muted)" }}>{c.db_type}</span></td>
                                    <td style={{ fontFamily: "monospace", fontSize: 12 }}>{c.account || c.host}{c.port ? `:${c.port}` : ""}</td>
                                    <td style={{ fontSize: 12 }}>{c.database_name || "—"}</td>
                                    <td>
                                        <button className="btn-sm btn-danger" onClick={() => handleDelete(c.id, c.name)}>Delete</button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}

            {/* ── Assign connections to users ── */}
            {!loading && connections.length > 0 && users.length > 0 && (
                <>
                    <h3 style={{ marginTop: 32, marginBottom: 12, fontSize: 15, fontWeight: 700 }}>Assign Connections to Users</h3>
                    <div className="admin-table-wrap">
                        <table className="admin-table">
                            <thead>
                                <tr>
                                    <th>User</th>
                                    <th>Email</th>
                                    <th>Role</th>
                                    <th>Assigned Connection</th>
                                </tr>
                            </thead>
                            <tbody>
                                {users.map(u => (
                                    <tr key={u.id}>
                                        <td style={{ fontWeight: 600 }}>{u.full_name}</td>
                                        <td>{u.email}</td>
                                        <td><span className={`role-badge role-${u.role}`}>{u.role}</span></td>
                                        <td>
                                            <select
                                                className="auth-input admin-select"
                                                style={{ minWidth: 200 }}
                                                value={u.connection_id ?? ""}
                                                onChange={e => handleAssign(u.id, e.target.value)}
                                            >
                                                <option value="">— No connection —</option>
                                                {connections.map(c => (
                                                    <option key={c.id} value={c.id}>{c.name}</option>
                                                ))}
                                            </select>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </>
            )}
        </div>
    );
}

// ── Users Tab ─────────────────────────────────────────────────────────────────

export default function AdminUsersPage() {
    const [activeTab, setActiveTab] = useState("users");
    const [users, setUsers] = useState([]);
    const [connections, setConnections] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [editingId, setEditingId] = useState(null);
    const [editRole, setEditRole] = useState("");

    const loadUsers = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const [data, conns] = await Promise.all([fetchUsers(), adminListConnections()]);
            setUsers(data);
            setConnections(conns || []);
        } catch (err) {
            setError(err.message || "Failed to load users");
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        loadUsers();
    }, [loadUsers]);

    const handleSaveRole = async (userId) => {
        try {
            await updateUser(userId, { role: editRole });
            setEditingId(null);
            loadUsers();
        } catch (err) {
            setError(err.message || "Failed to update user");
        }
    };

    const handleToggleActive = async (user) => {
        try {
            await updateUser(user.id, { is_active: !user.is_active });
            loadUsers();
        } catch (err) {
            setError(err.message || "Failed to update user");
        }
    };

    const handleDelete = async (userId) => {
        try {
            await deleteUser(userId);
            loadUsers();
        } catch (err) {
            setError(err.message || "Failed to delete user");
        }
    };

    const handleAssignConnection = async (userId, connectionId) => {
        setError("");
        try {
            const connId = connectionId === "" ? null : parseInt(connectionId, 10);
            await adminAssignConnection(userId, connId);
            loadUsers();
        } catch (err) {
            setError(err.message || "Failed to assign connection");
        }
    };

    return (
        <div className="module-shell">
            <div className="admin-header">
                <h2 className="admin-title">Administration</h2>
                <button className="btn-sm btn-outline" onClick={loadUsers} disabled={loading}>
                    Refresh
                </button>
            </div>

            {/* Tab bar */}
            <div style={{ display: "flex", gap: 4, marginBottom: 20, borderBottom: "1px solid var(--border)", paddingBottom: 0 }}>
                {[["users", "Users"], ["connections", "Connections"]].map(([key, label]) => (
                    <button
                        key={key}
                        onClick={() => setActiveTab(key)}
                        style={{
                            padding: "8px 20px",
                            fontSize: 13,
                            fontWeight: 600,
                            border: "none",
                            borderBottom: activeTab === key ? "2px solid var(--primary)" : "2px solid transparent",
                            background: "none",
                            color: activeTab === key ? "var(--primary)" : "var(--text-muted)",
                            cursor: "pointer",
                            transition: "color 0.15s",
                        }}
                    >
                        {label}
                    </button>
                ))}
            </div>

            {activeTab === "connections" && <ConnectionsTab />}

            {activeTab === "users" && (
                <>
                    {error && <div className="auth-error" style={{ marginBottom: 16 }}>{error}</div>}

                    {loading ? (
                        <p style={{ padding: 24, opacity: 0.6 }}>Loading users...</p>
                    ) : (
                        <div className="admin-table-wrap">
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th>ID</th>
                                        <th>Name</th>
                                        <th>Email</th>
                                        <th>Role</th>
                                        <th>Connection</th>
                                        <th>Active</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {users.map((u) => (
                                        <tr key={u.id} className={!u.is_active ? "row-inactive" : ""}>
                                            <td>{u.id}</td>
                                            <td>{u.full_name}</td>
                                            <td>{u.email}</td>
                                            <td>
                                                {editingId === u.id ? (
                                                    <div className="admin-role-edit">
                                                        <select
                                                            value={editRole}
                                                            onChange={(e) => setEditRole(e.target.value)}
                                                            className="auth-input admin-select"
                                                        >
                                                            {ROLE_OPTIONS.map((r) => (
                                                                <option key={r} value={r}>{r}</option>
                                                            ))}
                                                        </select>
                                                        <button className="btn-sm btn-primary" onClick={() => handleSaveRole(u.id)}>Save</button>
                                                        <button className="btn-sm btn-outline" onClick={() => setEditingId(null)}>Cancel</button>
                                                    </div>
                                                ) : (
                                                    <span
                                                        className={`role-badge role-${u.role}`}
                                                        onClick={() => { setEditingId(u.id); setEditRole(u.role); }}
                                                        title="Click to change role"
                                                        style={{ cursor: "pointer" }}
                                                    >
                                                        {u.role}
                                                    </span>
                                                )}
                                            </td>
                                            <td>
                                                <select
                                                    className="auth-input admin-select"
                                                    style={{ minWidth: 150, fontSize: 12 }}
                                                    value={u.connection_id ?? ""}
                                                    onChange={(e) => handleAssignConnection(u.id, e.target.value)}
                                                >
                                                    <option value="">— None —</option>
                                                    {connections.map((c) => (
                                                        <option key={c.id} value={c.id}>{c.name}</option>
                                                    ))}
                                                </select>
                                            </td>
                                            <td>
                                                <button
                                                    className={`btn-sm ${u.is_active ? "btn-success" : "btn-muted"}`}
                                                    onClick={() => handleToggleActive(u)}
                                                >
                                                    {u.is_active ? "Active" : "Inactive"}
                                                </button>
                                            </td>                                            <td>
                                                <button className="btn-sm btn-danger" onClick={() => handleDelete(u.id)} title="Deactivate user">
                                                    Deactivate
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </>
            )}
        </div>
    );
}
