export default function DatabaseList({
    databases = [],
    selectedDatabase = "",
    onSelectDatabase,
}) {
    return (
        <div className="sidebar-card">
            <h2>Database</h2>
            <div className="sidebar-list">
                {databases.length === 0 ? (
                    <div className="sidebar-empty">No databases found.</div>
                ) : (
                    databases.map((db) => (
                        <button
                            key={db}
                            className={`sidebar-item ${selectedDatabase === db ? "active" : ""}`}
                            onClick={() => onSelectDatabase(db)}
                        >
                            {db}
                        </button>
                    ))
                )}
            </div>
        </div>
    );
}