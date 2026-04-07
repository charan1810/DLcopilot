import { createContext, useContext, useState, useEffect, useCallback } from "react";
import {
    login as apiLogin,
    signup as apiSignup,
    fetchCurrentUser,
    getStoredToken,
    setStoredToken,
    clearStoredToken,
} from "../api/schemaApi";

const AuthContext = createContext(null);

export function useAuth() {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used inside AuthProvider");
    return ctx;
}

export function AuthProvider({ children }) {
    const [user, setUser] = useState(null);
    const [token, setToken] = useState(getStoredToken);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    // On mount — validate stored token
    useEffect(() => {
        if (!token) {
            setLoading(false);
            return;
        }
        fetchCurrentUser()
            .then((u) => setUser(u))
            .catch(() => {
                clearStoredToken();
                setToken(null);
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
            setUser(u);
            return u;
        } catch (err) {
            setError(err.message || "Login failed");
            throw err;
        }
    }, []);

    const signupFn = useCallback(async (email, full_name, password) => {
        setError("");
        try {
            const data = await apiSignup({ email, full_name, password });
            setStoredToken(data.access_token);
            setToken(data.access_token);
            const u = await fetchCurrentUser();
            setUser(u);
            return u;
        } catch (err) {
            setError(err.message || "Signup failed");
            throw err;
        }
    }, []);

    const logoutFn = useCallback(() => {
        clearStoredToken();
        setToken(null);
        setUser(null);
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
    };

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
