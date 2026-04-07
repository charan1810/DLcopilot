import { useEffect, useState, useCallback } from "react";
import { fetchUsers, updateUser, deleteUser } from "../api/schemaApi";

const ROLE_OPTIONS = ["admin", "developer", "tester"];

export default function AdminUsersPage() {
    const [users, setUsers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [editingId, setEditingId] = useState(null);
    const [editRole, setEditRole] = useState("");

    const loadUsers = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const data = await fetchUsers();
            setUsers(data);
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

    return (
        <div className="module-shell">
            <div className="admin-header">
                <h2 className="admin-title">User Management</h2>
                <button className="btn-sm btn-outline" onClick={loadUsers} disabled={loading}>
                    Refresh
                </button>
            </div>

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
                                                <button
                                                    className="btn-sm btn-primary"
                                                    onClick={() => handleSaveRole(u.id)}
                                                >
                                                    Save
                                                </button>
                                                <button
                                                    className="btn-sm btn-outline"
                                                    onClick={() => setEditingId(null)}
                                                >
                                                    Cancel
                                                </button>
                                            </div>
                                        ) : (
                                            <span
                                                className={`role-badge role-${u.role}`}
                                                onClick={() => {
                                                    setEditingId(u.id);
                                                    setEditRole(u.role);
                                                }}
                                                title="Click to change role"
                                                style={{ cursor: "pointer" }}
                                            >
                                                {u.role}
                                            </span>
                                        )}
                                    </td>
                                    <td>
                                        <button
                                            className={`btn-sm ${u.is_active ? "btn-success" : "btn-muted"}`}
                                            onClick={() => handleToggleActive(u)}
                                        >
                                            {u.is_active ? "Active" : "Inactive"}
                                        </button>
                                    </td>
                                    <td>
                                        <button
                                            className="btn-sm btn-danger"
                                            onClick={() => handleDelete(u.id)}
                                            title="Deactivate user"
                                        >
                                            Deactivate
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
