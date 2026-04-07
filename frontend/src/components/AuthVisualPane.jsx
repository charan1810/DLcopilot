import { useRef } from "react";
import AuthCanvasBackground from "./AuthCanvasBackground";

export default function AuthVisualPane() {
    const titleShellRef = useRef(null);

    const handleTitleMove = (event) => {
        const shell = titleShellRef.current;
        if (!shell) return;

        const rect = event.currentTarget.getBoundingClientRect();
        const nx = ((event.clientX - rect.left) / rect.width) * 2 - 1;
        const ny = ((event.clientY - rect.top) / rect.height) * 2 - 1;
        shell.style.setProperty("--rx", nx.toFixed(4));
        shell.style.setProperty("--ry", ny.toFixed(4));
    };

    const handleTitleEnter = () => {
        titleShellRef.current?.classList.add("is-hovered");
    };

    const handleTitleLeave = () => {
        if (!titleShellRef.current) return;
        titleShellRef.current.classList.remove("is-hovered");
        titleShellRef.current.style.setProperty("--rx", "0");
        titleShellRef.current.style.setProperty("--ry", "0");
    };

    return (
        <div className="auth-pane auth-pane-visual auth-pane-visual-interactive">
            <AuthCanvasBackground />

            <div className="auth-visual-overlay" aria-hidden="true">
                <div className="auth-visual-top">
                    <p className="auth-visual-kicker">INTELLIGENT DATA LIFECYCLE</p>

                    <div
                        ref={titleShellRef}
                        className="auth-visual-title-shell"
                        onMouseEnter={handleTitleEnter}
                        onMouseMove={handleTitleMove}
                        onMouseLeave={handleTitleLeave}
                    >
                        <svg className="auth-visual-title-svg" viewBox="0 0 1600 200" role="presentation">
                            <defs>
                                <linearGradient id="authTitleStrokeGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                                    <stop offset="0%" stopColor="#c4b5fd" stopOpacity="0.88" />
                                    <stop offset="45%" stopColor="#818cf8" stopOpacity="0.92" />
                                    <stop offset="100%" stopColor="#7dd3fc" stopOpacity="0.88" />
                                </linearGradient>
                            </defs>
                            <text
                                x="50%"
                                y="52%"
                                textAnchor="middle"
                                dominantBaseline="middle"
                                fill="none"
                                stroke="url(#authTitleStrokeGradient)"
                                strokeWidth="7"
                                paintOrder="stroke"
                                className="auth-visual-title-text"
                            >
                                DATA COPILOT
                            </text>
                        </svg>
                    </div>

                    <div className="auth-visual-divider" aria-hidden="true" />
                    <p className="auth-visual-quote">Analyze, transform, and ship trusted data faster.</p>
                </div>
            </div>
        </div>
    );
}
