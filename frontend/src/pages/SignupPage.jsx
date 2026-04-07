import { useState } from "react";
import { useAuth } from "../context/AuthContext";
import { Lock, Mail, User } from "lucide-react";
import AuthVisualPane from "../components/AuthVisualPane";

export default function SignupPage({ onSwitchToLogin }) {
    const { signup, error, setError } = useAuth();
    const [email, setEmail] = useState("");
    const [fullName, setFullName] = useState("");
    const [password, setPassword] = useState("");
    const [confirm, setConfirm] = useState("");
    const [busy, setBusy] = useState(false);

    const handleSubmit = async (e) => {
        e.preventDefault();
        if (!email || !fullName || !password || !confirm) {
            setError("Please fill in all fields");
            return;
        }
        if (password !== confirm) {
            setError("Passwords do not match");
            return;
        }
        if (password.length < 6) {
            setError("Password must be at least 6 characters");
            return;
        }
        setBusy(true);
        try {
            await signup(email, fullName, password);
        } catch {
            // error is set inside signup()
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

                    <h2 className="auth-heading">Create Account</h2>
                    <p className="auth-subtext">Get started with your free workspace</p>

                    {error && <div className="auth-error">{error}</div>}

                    <form onSubmit={handleSubmit} className="auth-form">
                        <div className="auth-field">
                            <label className="auth-label">Full Name</label>
                            <div className="auth-input-wrap">
                                <User size={16} className="auth-input-icon" />
                                <input
                                    type="text"
                                    className="auth-input"
                                    placeholder="John Doe"
                                    value={fullName}
                                    onChange={(e) => setFullName(e.target.value)}
                                    autoComplete="name"
                                    autoFocus
                                />
                            </div>
                        </div>

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
                                    placeholder="Min. 6 characters"
                                    value={password}
                                    onChange={(e) => setPassword(e.target.value)}
                                    autoComplete="new-password"
                                />
                            </div>
                        </div>

                        <div className="auth-field">
                            <label className="auth-label">Confirm Password</label>
                            <div className="auth-input-wrap">
                                <Lock size={16} className="auth-input-icon" />
                                <input
                                    type="password"
                                    className="auth-input"
                                    placeholder="Re-enter password"
                                    value={confirm}
                                    onChange={(e) => setConfirm(e.target.value)}
                                    autoComplete="new-password"
                                />
                            </div>
                        </div>

                        <button type="submit" className="auth-btn" disabled={busy}>
                            {busy ? "Creating account..." : "Create Account"}
                        </button>
                    </form>

                    <p className="auth-switch">
                        Already have an account?{" "}
                        <button type="button" className="auth-link" onClick={onSwitchToLogin}>
                            Sign in
                        </button>
                    </p>
                </div>
            </div>
        </div>
    );
}
