import { useState } from "react";
import { useAuth } from "../context/AuthContext";
import { Lock, Mail } from "lucide-react";
import AuthVisualPane from "../components/AuthVisualPane";

export default function LoginPage({ onSwitchToSignup }) {
    const { login, error, setError } = useAuth();
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [busy, setBusy] = useState(false);

    const handleSubmit = async (e) => {
        e.preventDefault();
        if (!email || !password) {
            setError("Please fill in all fields");
            return;
        }
        setBusy(true);
        try {
            await login(email, password);
        } catch {
            // error is set inside login()
        } finally {
            setBusy(false);
        }
    };

    return (
        <div className="auth-page auth-page-login auth-page-split">
            <AuthVisualPane />

            <div className="auth-pane auth-pane-form">
                <div className="auth-card">
                    <div className="auth-brand">
                        <div className="auth-brand-logo">DC</div>
                        <div>
                            <div className="auth-brand-title">Data Copilot</div>
                            <div className="auth-brand-subtitle">Lifecycle Platform</div>
                        </div>
                    </div>

                    <h2 className="auth-heading">Welcome back</h2>
                    <p className="auth-subtext">Sign in to continue to your workspace</p>

                    {error && <div className="auth-error">{error}</div>}

                    <form onSubmit={handleSubmit} className="auth-form">
                        <div className="auth-field">
                            <label className="auth-label">Email</label>
                            <div className="auth-input-wrap">
                                <Mail size={16} className="auth-input-icon" />
                                <input
                                    type="email"
                                    className="auth-input"
                                    placeholder="you@company.com"
                                    value={email}
                                    onChange={(e) => setEmail(e.target.value)}
                                    autoComplete="email"
                                    autoFocus
                                />
                            </div>
                        </div>

                        <div className="auth-field">
                            <label className="auth-label">Password</label>
                            <div className="auth-input-wrap">
                                <Lock size={16} className="auth-input-icon" />
                                <input
                                    type="password"
                                    className="auth-input"
                                    placeholder="••••••••"
                                    value={password}
                                    onChange={(e) => setPassword(e.target.value)}
                                    autoComplete="current-password"
                                />
                            </div>
                        </div>

                        <button type="submit" className="auth-btn" disabled={busy}>
                            {busy ? "Signing in..." : "Sign In"}
                        </button>
                    </form>

                    <p className="auth-switch">
                        Don&apos;t have an account?{" "}
                        <button type="button" className="auth-link" onClick={onSwitchToSignup}>
                            Create one
                        </button>
                    </p>
                </div>
            </div>
        </div>
    );
}
