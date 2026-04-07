import React from "react";

export default function DefinitionTab({ definition, loading, error }) {
    if (loading) {
        return <div className="object-details-empty">Loading definition...</div>;
    }

    if (error) {
        return <div className="object-details-error">{error}</div>;
    }

    return (
        <div className="definition-tab">
            <pre className="definition-code">
                {definition || "No definition available."}
            </pre>
        </div>
    );
}