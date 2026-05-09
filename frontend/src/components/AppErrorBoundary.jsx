import React from "react";

export default class AppErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, errorMessage: "" };
    }

    static getDerivedStateFromError(error) {
        return {
            hasError: true,
            errorMessage: error?.message || "Unexpected application error",
        };
    }

    componentDidCatch(error, errorInfo) {
        console.error("Application render error:", error, errorInfo);
    }

    handleReset = () => {
        this.setState({ hasError: false, errorMessage: "" });
        this.props.onReset?.();
    };

    render() {
        if (!this.state.hasError) {
            return this.props.children;
        }

        return (
            <div className="app-error-shell">
                <div className="app-error-card">
                    <div className="module-badge">Recovery Mode</div>
                    <h2 className="module-title">Frontend recovered from a page error</h2>
                    <p className="module-subtitle">
                        The current page failed to render, so the app stopped on a safe fallback instead of showing a blank screen.
                    </p>
                    <div className="pipeline-error-box app-error-detail">{this.state.errorMessage}</div>
                    <button type="button" className="pipeline-primary-btn" onClick={this.handleReset}>
                        Return To Explorer
                    </button>
                </div>
            </div>
        );
    }
}