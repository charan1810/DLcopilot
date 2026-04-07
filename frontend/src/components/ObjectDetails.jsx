import React, { useEffect, useMemo, useState } from "react";
import {
    fetchRelationships,
    fetchDefinition,
    fetchAiJoinSuggestions,
} from "../api/schemaApi";
import RelationshipsTab from "./RelationshipsTab";
import DefinitionTab from "./DefinitionTab";

function SuggestedRelationshipsInline({
    loading,
    error,
    suggestions = [],
    selectedObject,
    onRefresh,
    disabled,
}) {
    return (
        <div className="suggested-relationships-inline">
            <div className="inline-section-header">
                <div>
                    <h3>Suggested Relationships</h3>
                    <p className="section-caption">
                        Pattern-based hints for <strong>{selectedObject}</strong>. Validate against business logic before using them.
                    </p>
                </div>

                <button
                    type="button"
                    className="primary-btn secondary-size-btn"
                    onClick={onRefresh}
                    disabled={loading || disabled}
                >
                    {loading ? "Refreshing..." : "Refresh"}
                </button>
            </div>

            {error ? <div className="object-details-error">{error}</div> : null}

            {!error && loading ? (
                <div className="compact-empty-state">Loading suggested relationships...</div>
            ) : null}

            {!error && !loading && suggestions.length > 0 ? (
                <div className="relationship-hints-list">
                    {suggestions.slice(0, 6).map((item, idx) => (
                        <div
                            className="relationship-hint-item"
                            key={`${item.from_table}-${item.from_column}-${item.to_table}-${item.to_column}-${idx}`}
                        >
                            <div className="relationship-hint-main">
                                <div className="relationship-hint-path">
                                    {item.from_column}
                                    <span className="relationship-arrow">→</span>
                                    {item.to_table}.{item.to_column}
                                </div>
                                <div className="relationship-hint-score">Score: {item.score}</div>
                            </div>

                            {(item.reasons || []).length ? (
                                <div className="relationship-hint-reasons">
                                    {(item.reasons || []).join(", ")}
                                </div>
                            ) : null}
                        </div>
                    ))}
                </div>
            ) : null}

            {!error && !loading && suggestions.length === 0 ? (
                <div className="compact-empty-state">
                    No strong relationship hints found for this object.
                </div>
            ) : null}
        </div>
    );
}

export default function ObjectDetails({
    connectionId,
    databaseName,
    selectedSchema,
    selectedObject,
    columns = [],
    sampleData = [],
    objectType = "",
}) {
    const tabs = useMemo(() => {
        const base = ["columns", "data", "relationships"];
        if (objectType === "VIEW") {
            base.push("definition");
        }
        return base;
    }, [objectType]);

    const [activeTab, setActiveTab] = useState("columns");

    const [relationshipsData, setRelationshipsData] = useState(null);
    const [relationshipsLoading, setRelationshipsLoading] = useState(false);
    const [relationshipsError, setRelationshipsError] = useState("");

    const [suggestedRelationships, setSuggestedRelationships] = useState(null);
    const [suggestedRelationshipsLoading, setSuggestedRelationshipsLoading] = useState(false);
    const [suggestedRelationshipsError, setSuggestedRelationshipsError] = useState("");

    const [definition, setDefinition] = useState("");
    const [definitionLoading, setDefinitionLoading] = useState(false);
    const [definitionError, setDefinitionError] = useState("");

    useEffect(() => {
        setActiveTab("columns");
        setRelationshipsData(null);
        setRelationshipsError("");
        setSuggestedRelationships(null);
        setSuggestedRelationshipsError("");
        setDefinition("");
        setDefinitionError("");
    }, [connectionId, databaseName, selectedSchema, selectedObject]);

    const loadVerifiedRelationships = async () => {
        if (!connectionId || !databaseName || !selectedSchema || !selectedObject) return;

        setRelationshipsLoading(true);
        setRelationshipsError("");

        try {
            const data = await fetchRelationships(
                connectionId,
                databaseName,
                selectedSchema,
                selectedObject
            );
            setRelationshipsData(data);
        } catch (err) {
            setRelationshipsError(err.message || "Failed to load relationships");
        } finally {
            setRelationshipsLoading(false);
        }
    };

    const loadSuggestedRelationships = async (forceRefresh = false) => {
        if (!connectionId || !databaseName || !selectedSchema || !selectedObject) return;

        if (!forceRefresh && suggestedRelationships) return;

        setSuggestedRelationshipsLoading(true);
        setSuggestedRelationshipsError("");

        try {
            const data = await fetchAiJoinSuggestions(
                connectionId,
                databaseName,
                selectedSchema,
                selectedObject
            );
            setSuggestedRelationships(data);
        } catch (err) {
            setSuggestedRelationshipsError(err.message || "Failed to load suggested relationships");
        } finally {
            setSuggestedRelationshipsLoading(false);
        }
    };

    const loadDefinition = async () => {
        if (!connectionId || !databaseName || !selectedSchema || !selectedObject) return;
        if (objectType !== "VIEW") return;

        setDefinitionLoading(true);
        setDefinitionError("");

        try {
            const data = await fetchDefinition(
                connectionId,
                databaseName,
                selectedSchema,
                selectedObject
            );
            setDefinition(data.definition || "");
        } catch (err) {
            setDefinitionError(err.message || "Failed to load definition");
        } finally {
            setDefinitionLoading(false);
        }
    };

    useEffect(() => {
        if (!connectionId || !databaseName || !selectedSchema || !selectedObject) return;

        if (activeTab === "relationships") {
            if (!relationshipsData && !relationshipsLoading) {
                loadVerifiedRelationships();
            }

            if (!suggestedRelationships && !suggestedRelationshipsLoading) {
                loadSuggestedRelationships();
            }
        }

        if (
            activeTab === "definition" &&
            objectType === "VIEW" &&
            !definition &&
            !definitionLoading
        ) {
            loadDefinition();
        }
    }, [
        activeTab,
        connectionId,
        databaseName,
        selectedSchema,
        selectedObject,
        objectType,
        relationshipsData,
        relationshipsLoading,
        suggestedRelationships,
        suggestedRelationshipsLoading,
        definition,
        definitionLoading,
    ]);

    if (!selectedObject) {
        return (
            <div className="object-details-empty big-empty">
                <div className="empty-title">No object selected</div>
                <div className="empty-subtitle">
                    Choose a schema and table/view to inspect columns, sample data, relationships, and definitions.
                </div>
            </div>
        );
    }

    const compactSuggestions = suggestedRelationships?.heuristic_relationships || [];

    return (
        <div className="object-details">
            <div className="object-details-header">
                <div>
                    <h2>{selectedObject}</h2>
                    <div className="object-details-subtitle">
                        {databaseName} • {selectedSchema} {objectType ? `• ${objectType}` : ""}
                    </div>
                </div>
            </div>

            <div className="object-details-tabs">
                {tabs.map((tab) => (
                    <button
                        key={tab}
                        className={`object-details-tab ${activeTab === tab ? "active" : ""}`}
                        onClick={() => setActiveTab(tab)}
                        type="button"
                    >
                        {tab.charAt(0).toUpperCase() + tab.slice(1)}
                    </button>
                ))}
            </div>

            <div className="object-details-content">
                {activeTab === "columns" && (
                    <div className="columns-tab">
                        {columns.length ? (
                            <div className="relationship-table">
                                <div className="relationship-row relationship-header relationship-row-4">
                                    <div>Column</div>
                                    <div>Data Type</div>
                                    <div>Nullable</div>
                                    <div>Default</div>
                                </div>
                                {columns.map((col, idx) => (
                                    <div
                                        className="relationship-row relationship-row-4"
                                        key={`${col.column_name}-${idx}`}
                                    >
                                        <div>{col.column_name}</div>
                                        <div>{col.data_type}</div>
                                        <div>{col.is_nullable}</div>
                                        <div>{col.column_default ?? ""}</div>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="object-details-empty">No columns available.</div>
                        )}
                    </div>
                )}

                {activeTab === "data" && (
                    <div className="data-tab">
                        {sampleData.length ? (
                            <div className="sample-table-wrapper">
                                <table className="sample-table">
                                    <thead>
                                        <tr>
                                            {Object.keys(sampleData[0]).map((key) => (
                                                <th key={key}>{key}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sampleData.map((row, rowIdx) => (
                                            <tr key={rowIdx}>
                                                {Object.keys(sampleData[0]).map((key) => (
                                                    <td key={key}>{String(row[key] ?? "")}</td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        ) : (
                            <div className="object-details-empty">No sample data available.</div>
                        )}
                    </div>
                )}

                {activeTab === "relationships" && (
                    <div className="relationships-tab-stack">
                        <RelationshipsTab
                            data={relationshipsData}
                            loading={relationshipsLoading}
                            error={relationshipsError}
                        />

                        <SuggestedRelationshipsInline
                            loading={suggestedRelationshipsLoading}
                            error={suggestedRelationshipsError}
                            suggestions={compactSuggestions}
                            selectedObject={selectedObject}
                            onRefresh={() => loadSuggestedRelationships(true)}
                            disabled={!connectionId || !databaseName || !selectedSchema || !selectedObject}
                        />
                    </div>
                )}

                {activeTab === "definition" && objectType === "VIEW" && (
                    <DefinitionTab
                        definition={definition}
                        loading={definitionLoading}
                        error={definitionError}
                    />
                )}
            </div>
        </div>
    );
}