export default function SchemaSidebar({
    schemas = [],
    selectedSchema = "",
    onSelectSchema,
}) {
    return (
        <div className="sidebar-card">
            <h2>Schemas</h2>

            <div className="sidebar-list">
                {schemas.length === 0 ? (
                    <div className="sidebar-empty">No schemas found.</div>
                ) : (
                    schemas.map((schema) => (
                        <button
                            key={schema}
                            type="button"
                            className={`sidebar-item ${selectedSchema === schema ? "active" : ""}`}
                            onClick={() => onSelectSchema(schema)}
                        >
                            {schema}
                        </button>
                    ))
                )}
            </div>
        </div>
    );
}