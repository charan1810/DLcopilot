import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

const AppContext = createContext(null);

function readStorage(key, fallback, storage) {
    try {
        const raw = storage.getItem(key);
        return raw ? JSON.parse(raw) : fallback;
    } catch {
        return fallback;
    }
}

function usePersistentState(key, initialValue, storageType = "local") {
    const storage = storageType === "session" ? window.sessionStorage : window.localStorage;

    const [value, setValue] = useState(() => readStorage(key, initialValue, storage));

    useEffect(() => {
        try {
            storage.setItem(key, JSON.stringify(value));
        } catch {
            // ignore storage issues
        }
    }, [key, value, storage]);

    return [value, setValue];
}

const DEFAULT_CONNECTION_PAYLOAD = {
    name: "Local Postgres",
    db_type: "postgres",
    host: "localhost",
    port: "5433",
    database_name: "dlcopilot",
    schema_name: "public",
    username: "postgres",
    account: "",
    warehouse: "",
    role: "",
};

const DEFAULT_EXPLORER_CACHE = {
    databases: [],
    schemas: [],
    objects: [],
    objectDetails: null,
    sampleData: [],
    autoQuery: "",
};

const LINEAGE_TABS = ["lineage", "transformations", "pipeline-builder"];

function createEmptySelectionSession() {
    return {
        selectedDatabase: "",
        selectedSchema: "",
        selectedObject: "",
    };
}

function createEmptyLineageViewSession() {
    return {
        lineage: null,
        fetchedAt: null,
    };
}

function createEmptyTransformationSession() {
    return {
        sqlPrompt: "",
        sqlResult: null,
        sqlError: "",
        selectedJoinTables: [],
    };
}

function createEmptyPipelineBuilderSession() {
    return {
        pipelines: [],
        selectedPipelineId: null,
        selectedPipeline: null,
        runs: [],
        selectedRun: null,
        message: "",
        error: "",
        pipelineForm: null,
        stepForm: null,
    };
}

function createEmptyLineageUiSession() {
    return {
        activeTab: "lineage",
        tabSelections: {
            lineage: createEmptySelectionSession(),
            transformations: createEmptySelectionSession(),
            "pipeline-builder": createEmptySelectionSession(),
        },
    };
}

export function AppProvider({ children }) {
    const [connectionId, setConnectionId] = usePersistentState("dc.connectionId", null);
    const [selectedDatabase, setSelectedDatabase] = usePersistentState("dc.selectedDatabase", "");
    const [selectedSchema, setSelectedSchema] = usePersistentState("dc.selectedSchema", "");
    const [selectedObject, setSelectedObject] = usePersistentState("dc.selectedObject", "");

    const [connectionPayload, setConnectionPayload] = usePersistentState(
        "dc.connectionPayload",
        DEFAULT_CONNECTION_PAYLOAD
    );

    const [sessionPassword, setSessionPassword] = usePersistentState(
        "dc.sessionPassword",
        "",
        "session"
    );

    const [explorerCache, setExplorerCache] = usePersistentState(
        "dc.explorerCache",
        DEFAULT_EXPLORER_CACHE
    );

    const [lineageViewCache, setLineageViewCache] = usePersistentState(
        "dc.lineageViewCache",
        {}
    );

    const [transformationCache, setTransformationCache] = usePersistentState(
        "dc.transformationCache",
        {}
    );

    const [pipelineBuilderCache, setPipelineBuilderCache] = usePersistentState(
        "dc.pipelineBuilderCache",
        {}
    );

    const [lineageUiCache, setLineageUiCache] = usePersistentState(
        "dc.lineageUiCache",
        {}
    );

    const [queryRunnerQuery, setQueryRunnerQuery] = usePersistentState(
        "dc.queryRunnerQuery",
        ""
    );

    const [activePage, setActivePage] = usePersistentState(
        "dc.activePage",
        "explorer"
    );

    const [lineageUiSession, setLineageUiSession] = usePersistentState(
        "dc.lineageUiSession",
        createEmptyLineageUiSession()
    );

    const currentLineageActiveTab = lineageUiSession?.activeTab || "lineage";

    const currentLineageSelection = useMemo(() => {
        return lineageUiSession?.tabSelections?.[currentLineageActiveTab] || createEmptySelectionSession();
    }, [lineageUiSession, currentLineageActiveTab]);

    const currentLineageKey = useMemo(() => {
        const { selectedDatabase: dbName, selectedSchema: schemaName, selectedObject: objectName } = currentLineageSelection;
        if (!dbName || !schemaName || !objectName) return "";
        return `${currentLineageActiveTab}|${dbName}.${schemaName}.${objectName}`;
    }, [currentLineageActiveTab, currentLineageSelection]);

    const currentLineageViewSession = useMemo(() => {
        if (!currentLineageKey) return createEmptyLineageViewSession();
        return lineageViewCache[currentLineageKey] || createEmptyLineageViewSession();
    }, [currentLineageKey, lineageViewCache]);

    const currentTransformationSession = useMemo(() => {
        if (!currentLineageKey) return createEmptyTransformationSession();
        return transformationCache[currentLineageKey] || createEmptyTransformationSession();
    }, [currentLineageKey, transformationCache]);

    const currentPipelineBuilderSession = useMemo(() => {
        if (!currentLineageKey) return createEmptyPipelineBuilderSession();
        return pipelineBuilderCache[currentLineageKey] || createEmptyPipelineBuilderSession();
    }, [currentLineageKey, pipelineBuilderCache]);

    const currentLineageUiSession = useMemo(() => {
        if (!currentLineageKey) return createEmptyLineageUiSession();
        return lineageUiCache[currentLineageKey] || createEmptyLineageUiSession();
    }, [currentLineageKey, lineageUiCache]);

    const updateLineageTabSelection = useCallback((tab, patch) => {
        if (!LINEAGE_TABS.includes(tab)) return;

        setLineageUiSession((prev) => {
            const existingSelection = prev?.tabSelections?.[tab] || createEmptySelectionSession();
            const nextSelection =
                typeof patch === "function"
                    ? patch(existingSelection)
                    : { ...existingSelection, ...patch };

            return {
                ...prev,
                tabSelections: {
                    ...(prev?.tabSelections || {}),
                    [tab]: nextSelection,
                },
            };
        });
    }, [setLineageUiSession]);

    const updateCurrentLineageSelection = useCallback((patch) => {
        updateLineageTabSelection(currentLineageActiveTab, patch);
    }, [currentLineageActiveTab, updateLineageTabSelection]);

    const setLineageActiveTab = useCallback((tab, seedSelection = null) => {
        if (!LINEAGE_TABS.includes(tab)) return;

        setLineageUiSession((prev) => {
            const currentSelections = prev?.tabSelections || {};
            const nextTabSelection = currentSelections[tab] || createEmptySelectionSession();
            const hasExistingSelection =
                !!nextTabSelection.selectedDatabase && !!nextTabSelection.selectedSchema && !!nextTabSelection.selectedObject;
            const shouldSeed =
                !hasExistingSelection &&
                seedSelection?.selectedDatabase &&
                seedSelection?.selectedSchema &&
                seedSelection?.selectedObject;

            return {
                ...prev,
                activeTab: tab,
                tabSelections: {
                    ...currentSelections,
                    [tab]: shouldSeed
                        ? {
                            selectedDatabase: seedSelection.selectedDatabase,
                            selectedSchema: seedSelection.selectedSchema,
                            selectedObject: seedSelection.selectedObject,
                        }
                        : nextTabSelection,
                },
            };
        });
    }, [setLineageUiSession]);

    const updateCurrentLineageViewSession = useCallback((patch) => {
        if (!currentLineageKey) return;

        setLineageViewCache((prev) => {
            const existing = prev[currentLineageKey] || createEmptyLineageViewSession();
            const nextValue =
                typeof patch === "function" ? patch(existing) : { ...existing, ...patch };

            return {
                ...prev,
                [currentLineageKey]: nextValue,
            };
        });
    }, [currentLineageKey, setLineageViewCache]);

    const updateCurrentTransformationSession = useCallback((patch) => {
        if (!currentLineageKey) return;

        setTransformationCache((prev) => {
            const existing = prev[currentLineageKey] || createEmptyTransformationSession();
            const nextValue =
                typeof patch === "function" ? patch(existing) : { ...existing, ...patch };

            return {
                ...prev,
                [currentLineageKey]: nextValue,
            };
        });
    }, [currentLineageKey, setTransformationCache]);

    const updateCurrentPipelineBuilderSession = useCallback((patch) => {
        if (!currentLineageKey) return;

        setPipelineBuilderCache((prev) => {
            const existing = prev[currentLineageKey] || createEmptyPipelineBuilderSession();
            const nextValue =
                typeof patch === "function" ? patch(existing) : { ...existing, ...patch };

            return {
                ...prev,
                [currentLineageKey]: nextValue,
            };
        });
    }, [currentLineageKey, setPipelineBuilderCache]);

    const updateCurrentLineageUiSession = useCallback((patch) => {
        if (!currentLineageKey) return;

        setLineageUiCache((prev) => {
            const existing = prev[currentLineageKey] || createEmptyLineageUiSession();
            const nextValue =
                typeof patch === "function" ? patch(existing) : { ...existing, ...patch };

            return {
                ...prev,
                [currentLineageKey]: nextValue,
            };
        });
    }, [currentLineageKey, setLineageUiCache]);

    const clearCurrentLineageSession = useCallback(() => {
        if (!currentLineageKey) return;

        setLineageViewCache((prev) => ({
            ...prev,
            [currentLineageKey]: createEmptyLineageViewSession(),
        }));
        setTransformationCache((prev) => ({
            ...prev,
            [currentLineageKey]: createEmptyTransformationSession(),
        }));
        setPipelineBuilderCache((prev) => ({
            ...prev,
            [currentLineageKey]: createEmptyPipelineBuilderSession(),
        }));
        setLineageUiCache((prev) => ({
            ...prev,
            [currentLineageKey]: createEmptyLineageUiSession(),
        }));
    }, [currentLineageKey, setLineageViewCache, setTransformationCache, setPipelineBuilderCache, setLineageUiCache]);

    const clearAllLineageCache = useCallback(() => {
        setLineageViewCache({});
        setTransformationCache({});
        setPipelineBuilderCache({});
        setLineageUiCache({});
        setLineageUiSession(createEmptyLineageUiSession());
    }, [setLineageViewCache, setTransformationCache, setPipelineBuilderCache, setLineageUiCache, setLineageUiSession]);

    const resetSelections = useCallback(() => {
        setSelectedDatabase("");
        setSelectedSchema("");
        setSelectedObject("");
    }, [setSelectedDatabase, setSelectedSchema, setSelectedObject]);

    const clearExplorerCache = useCallback(() => {
        setExplorerCache(DEFAULT_EXPLORER_CACHE);
    }, [setExplorerCache]);

    const clearAllCachedState = useCallback(() => {
        setConnectionId(null);
        setSelectedDatabase("");
        setSelectedSchema("");
        setSelectedObject("");
        setConnectionPayload(DEFAULT_CONNECTION_PAYLOAD);
        setSessionPassword("");
        setExplorerCache(DEFAULT_EXPLORER_CACHE);
        setLineageViewCache({});
        setTransformationCache({});
        setPipelineBuilderCache({});
        setLineageUiCache({});
        setLineageUiSession(createEmptyLineageUiSession());
        setQueryRunnerQuery("");
        setActivePage("explorer");
    }, [
        setConnectionId,
        setSelectedDatabase,
        setSelectedSchema,
        setSelectedObject,
        setConnectionPayload,
        setSessionPassword,
        setExplorerCache,
        setLineageViewCache,
        setTransformationCache,
        setPipelineBuilderCache,
        setLineageUiCache,
        setLineageUiSession,
        setQueryRunnerQuery,
        setActivePage,
    ]);

    const value = useMemo(
        () => ({
            connectionId,
            setConnectionId,

            selectedDatabase,
            setSelectedDatabase,
            selectedSchema,
            setSelectedSchema,
            selectedObject,
            setSelectedObject,

            currentLineageActiveTab,
            currentLineageSelection,
            updateCurrentLineageSelection,
            updateLineageTabSelection,
            setLineageActiveTab,

            connectionPayload,
            setConnectionPayload,
            sessionPassword,
            setSessionPassword,

            explorerCache,
            setExplorerCache,

            lineageViewCache,
            setLineageViewCache,
            transformationCache,
            setTransformationCache,
            pipelineBuilderCache,
            setPipelineBuilderCache,
            lineageUiCache,
            setLineageUiCache,
            lineageUiSession,
            setLineageUiSession,
            currentLineageKey,
            currentLineageViewSession,
            currentTransformationSession,
            currentPipelineBuilderSession,
            currentLineageUiSession,
            updateCurrentLineageViewSession,
            updateCurrentTransformationSession,
            updateCurrentPipelineBuilderSession,
            updateCurrentLineageUiSession,
            clearCurrentLineageSession,
            clearAllLineageCache,
            createEmptyLineageViewSession,
            createEmptyTransformationSession,
            createEmptyPipelineBuilderSession,

            queryRunnerQuery,
            setQueryRunnerQuery,
            activePage,
            setActivePage,

            resetSelections,
            clearExplorerCache,
            clearAllCachedState,
        }),
        [
            connectionId,
            selectedDatabase,
            selectedSchema,
            selectedObject,
            currentLineageActiveTab,
            currentLineageSelection,
            connectionPayload,
            sessionPassword,
            explorerCache,
            lineageViewCache,
            transformationCache,
            pipelineBuilderCache,
            lineageUiCache,
            lineageUiSession,
            currentLineageKey,
            currentLineageViewSession,
            currentTransformationSession,
            currentPipelineBuilderSession,
            currentLineageUiSession,
            updateLineageTabSelection,
            updateCurrentLineageSelection,
            setLineageActiveTab,
            queryRunnerQuery,
            activePage,
            updateCurrentLineageViewSession,
            updateCurrentTransformationSession,
            updateCurrentPipelineBuilderSession,
            updateCurrentLineageUiSession,
            clearCurrentLineageSession,
            clearAllLineageCache,
            resetSelections,
            clearExplorerCache,
            clearAllCachedState,
        ]
    );

    return <AppContext.Provider value={value}>{children}</AppContext.Provider>;
}

export function useAppContext() {
    const context = useContext(AppContext);
    if (!context) {
        throw new Error("useAppContext must be used inside AppProvider");
    }
    return context;
}