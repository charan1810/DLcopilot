import { createContext, useContext, useState, useEffect, useCallback } from "react";
import {
    login as apiLogin,
    signup as apiSignup,
    fetchCurrentUser,
    getStoredToken,
    setStoredToken,
    clearStoredToken,
    fetchSavedConnections,
} from "../api/schemaApi";

const AuthContext = createContext(null);

// Stored separately so AppContext can read it synchronously on boot
const ACTIVE_CONN_KEY = "dc.activeConnection";

function storeActiveConnection(conn) {
    try {
        localStorage.setItem(ACTIVE_CONN_KEY, JSON.stringify(conn));
    } catch { /* ignore */ }
}

function clearActiveConnection() {
    try { localStorage.removeItem(ACTIVE_CONN_KEY); } catch { /* ignore */ }
}

export function getStoredActiveConnection() {
    try {
        const raw = localStorage.getItem(ACTIVE_CONN_KEY);
        return raw ? JSON.parse(raw) : null;
    } catch { return null; }
}

export function useAuth() {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used inside AuthProvider");
    return ctx;
}

async function resolveConnectionForUser(user) {
    if (!user?.connection_id) return null;
    try {
        const list = await fetchSavedConnections();
        return list.find((c) => c.id === user.connection_id) || null;
    } catch { return null; }
}

export function AuthProvider({ children }) {
    const [user, setUser] = useState(null);
    const [token, setToken] = useState(getStoredToken);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    // The resolved connection object for the current user
    const [activeConnection, setActiveConnection] = useState(getStoredActiveConnection);

    const applyUser = useCallback(async (u) => {
        setUser(u);
        if (u?.connection_id) {
            const conn = await resolveConnectionForUser(u);
            setActiveConnection(conn);
            storeActiveConnection(conn);
        } else {
            setActiveConnection(null);
            clearActiveConnection();
        }
    }, []);

    // On mount — validate stored token
    useEffect(() => {
        if (!token) {
            setLoading(false);
            return;
        }
        fetchCurrentUser()
            .then((u) => applyUser(u))
            .catch(() => {
                clearStoredToken();
                setToken(null);
                clearActiveConnection();
            })
            .finally(() => setLoading(false));
    }, []); // eslint-disable-line react-hooks/exhaustive-deps

    const loginFn = useCallback(async (email, password) => {
        setError("");
        try {
            const data = await apiLogin({ email, password });
            setStoredToken(data.access_token);
            setToken(data.access_token);
            const u = await fetchCurrentUser();
            await applyUser(u);
            return u;
        } catch (err) {
            setError(err.message || "Login failed");
            throw err;
        }
    }, [applyUser]);

    const signupFn = useCallback(async (email, full_name, password) => {
        setError("");
        try {
            const data = await apiSignup({ email, full_name, password });
            setStoredToken(data.access_token);
            setToken(data.access_token);
            const u = await fetchCurrentUser();
            await applyUser(u);
            return u;
        } catch (err) {
            setError(err.message || "Signup failed");
            throw err;
        }
    }, [applyUser]);

    const logoutFn = useCallback(() => {
        clearStoredToken();
        clearActiveConnection();
        setToken(null);
        setUser(null);
        setActiveConnection(null);
    }, []);

    const value = {
        user,
        token,
        loading,
        error,
        isAuthenticated: !!user,
        login: loginFn,
        signup: signupFn,
        logout: logoutFn,
        setError,
        activeConnection,   // the full connection object assigned by admin
    };

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
