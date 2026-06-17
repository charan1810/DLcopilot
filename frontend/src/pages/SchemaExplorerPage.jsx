import { useEffect, useState } from "react";
import ConnectionForm from "../components/ConnectionForm";
import DatabaseList from "../components/DatabaseList";
import SchemaSidebar from "../components/SchemaSidebar";
import ObjectList from "../components/ObjectList";
import ObjectDetails from "../components/ObjectDetails";
import QueryRunner from "../components/QueryRunner";
import {
    fetchDatabases,
    fetchSchemas,
    fetchObjects,
    fetchObjectDetails,
    fetchObjectSampleData,
    saveConnection,
} from "../api/schemaApi";
import { useAppContext } from "../context/AppContext";

export default function SchemaExplorerPage() {
    const {
        connectionId,
        setConnectionId,
        selectedDatabase,
        setSelectedDatabase,
        selectedSchema,
        setSelectedSchema,
        selectedObject,
        setSelectedObject,
        connectionPayload,
        sessionPassword,
        explorerCache,
        setExplorerCache,
        resetSelections,
    } = useAppContext();

    const [loading, setLoading] = useState(false);
    const [connectionError, setConnectionError] = useState("");

    const databases = explorerCache.databases || [];
    const schemas = explorerCache.schemas || [];
    const objects = explorerCache.objects || [];
    const objectDetails = explorerCache.objectDetails || null;
    const sampleData = explorerCache.sampleData || [];
    const autoQuery = explorerCache.autoQuery || "";
    const setCache = (patch) => {
        setExplorerCache((prev) => ({ ...prev, ...patch }));
    };

    const resetBelowDatabase = () => {
        setCache({
            schemas: [],
            objects: [],
            objectDetails: null,
            sampleData: [],
            autoQuery: "",
        });
        setSelectedSchema("");
        setSelectedObject("");
    };

    const resetBelowSchema = () => {
        setCache({
            objects: [],
            objectDetails: null,
            sampleData: [],
            autoQuery: "",
        });
        setSelectedObject("");
    };

    const ensureActiveBackendConnection = async (forceNew = false) => {
        if (!forceNew && connectionId) return connectionId;

        if (!connectionPayload?.host || !connectionPayload?.username) {
            return null;
        }

        const payload = {
            ...connectionPayload,
            password: sessionPassword || "",
        };

        const saved = await saveConnection(payload);
        setConnectionId(saved.id);
        return saved.id;
    };

    const runWithReconnect = async (apiCall) => {
        let activeId = await ensureActiveBackendConnection(false);

        if (!activeId) {
            throw new Error("No active connection available");
        }

        try {
            return await apiCall(activeId);
        } catch (err) {
            const msg = err?.message || "";

            if (msg.includes("Connection not found")) {
                activeId = await ensureActiveBackendConnection(true);

                if (!activeId) {
                    throw new Error("No active connection available");
                }

                return await apiCall(activeId);
            }

            throw err;
        }
    };

    const safeFetchDatabases = async (idToUse) => {
        try {
            return await fetchDatabases(idToUse);
        } catch (err) {
            const errorMsg = err.message || "";
            // Handle password-related errors
            if (
                errorMsg.includes("Connection not found") ||
                errorMsg.includes("no password supplied") ||
                errorMsg.includes("authentication failed") ||
                errorMsg.includes("FATAL")
            ) {
                // Try to re-save with current session password and retry
                if (connectionPayload && sessionPassword) {
                    try {
                        const payload = {
                            ...connectionPayload,
                            password: sessionPassword || "",
                        };
                        const saved = await saveConnection(payload);
                        setConnectionId(saved.id);
                        return await fetchDatabases(saved.id);
                    } catch (saveErr) {
                        // If save fails, throw the original error for user to handle
                        throw err;
                    }
                }
            }
            throw err;
        }
    };

    const handleConnectionSaved = async (id) => {
        setConnectionId(id);
        setConnectionError("");
        resetSelections();

        setCache({
            databases: [],
            schemas: [],
            objects: [],
            objectDetails: null,
            sampleData: [],
            autoQuery: "",
        });

        setLoading(true);
        try {
            const res = await fetchDatabases(id);
            setCache({ databases: res.databases || [] });
        } catch (err) {
            const errorMsg = err.message || "Failed to connect to database";
            console.error("Failed to load databases:", err);
            setConnectionError(errorMsg);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        const hydrateExplorer = async () => {
            if (databases.length > 0) return;

            const activeId = await ensureActiveBackendConnection();
            if (!activeId) return;

            setLoading(true);
            setConnectionError("");
            try {
                const res = await safeFetchDatabases(activeId);
                setCache({ databases: res.databases || [] });
            } catch (err) {
                // Log error and display to user
                const errorMsg = err.message || "Failed to connect to database";
                if (
                    errorMsg.includes("no password supplied") ||
                    errorMsg.includes("authentication failed") ||
                    errorMsg.includes("FATAL")
                ) {
                    // Assigned connections may require a session password on first use.
                    setConnectionError("");
                    return;
                }
                console.error("Failed to hydrate explorer:", err);
                setConnectionError(errorMsg);
                // Clear loading state so UI can show the connection form
            } finally {
                setLoading(false);
            }
        };

        hydrateExplorer();
    }, []);

    const handleDatabaseSelect = async (db) => {
        setSelectedDatabase(db);
        resetBelowDatabase();

        setLoading(true);
        try {
            const res = await runWithReconnect((activeId) =>
                fetchSchemas(activeId, db)
            );
            setCache({ schemas: res.schemas || [] });
        } finally {
            setLoading(false);
        }
    };

    const handleSchemaSelect = async (schema) => {
        setSelectedSchema(schema);
        resetBelowSchema();

        setLoading(true);
        try {
            const res = await runWithReconnect((activeId) =>
                fetchObjects(activeId, selectedDatabase, schema)
            );
            setCache({ objects: res.objects || [] });
        } finally {
            setLoading(false);
        }
    };

    const handleObjectSelect = async (objectName) => {
        setSelectedObject(objectName);

        const nextQuery = `SELECT * FROM "${selectedSchema}"."${objectName}"`;
        setCache({
            autoQuery: nextQuery,
        });

        setLoading(true);
        try {
            const [detailsRes, sampleRes] = await runWithReconnect((activeId) =>
                Promise.all([
                    fetchObjectDetails(activeId, selectedDatabase, selectedSchema, objectName),
                    fetchObjectSampleData(activeId, selectedDatabase, selectedSchema, objectName, 25),
                ])
            );

            const rowsAsObjects = (sampleRes.rows || []).map((row) => {
                const obj = {};
                (sampleRes.columns || []).forEach((col, idx) => {
                    obj[col] = row[idx];
                });
                return obj;
            });

            setCache({
                objectDetails: detailsRes,
                sampleData: rowsAsObjects,
            });
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="page">
            <div className="module-hero compact-hero">
                <div className="module-badge">Engineering Flow</div>
                <h1 className="module-title">Explorer</h1>
                <p className="module-subtitle">
                    Connect to a database server, browse schemas and objects, inspect metadata,
                    and pass the same context into ETL / Lineage and Insights.
                </p>
            </div>

            <ConnectionForm onConnectionSaved={handleConnectionSaved} />

            {loading && <div className="loading-banner">Loading...</div>}
            {connectionError && (
                <div className="form-error" style={{ margin: "20px" }}>
                    <strong>Connection Error:</strong> {connectionError}
                    <br />
                    <small>Please check your connection settings and try again.</small>
                </div>
            )}

            <div className="explorer-top-grid">
                <div className="explorer-card fixed-top-card">
                    <DatabaseList
                        databases={databases}
                        selectedDatabase={selectedDatabase}
                        onSelectDatabase={handleDatabaseSelect}
                    />
                </div>

                <div className="explorer-card fixed-top-card">
                    <SchemaSidebar
                        schemas={schemas}
                        selectedSchema={selectedSchema}
                        onSelectSchema={handleSchemaSelect}
                    />
                </div>

                <div className="explorer-card fixed-top-card explorer-objects-col">
                    <ObjectList
                        objects={objects}
                        selectedObject={selectedObject}
                        onSelectObject={handleObjectSelect}
                    />
                </div>
            </div>

            <div className="details-card">
                <ObjectDetails
                    connectionId={connectionId}
                    databaseName={selectedDatabase}
                    selectedSchema={selectedSchema}
                    selectedObject={selectedObject}
                    columns={objectDetails?.columns || []}
                    objectType={objectDetails?.object_type || ""}
                    sampleData={sampleData}
                />
            </div>

            <div className="query-card">
                <QueryRunner
                    connectionId={connectionId}
                    databaseName={selectedDatabase}
                    autoQuery={autoQuery}
                />
            </div>
        </div>
    );
}