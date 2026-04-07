export default function ObjectList({
    objects = [],
    selectedObject = "",
    onSelectObject,
}) {
    return (
        <div className="sidebar-card">
            <h2>Tables / Views</h2>

            <div className="sidebar-list">
                {objects.length === 0 ? (
                    <div className="sidebar-empty">No objects found.</div>
                ) : (
                    objects.map((obj) => (
                        <button
                            key={obj.name}
                            type="button"
                            className={`sidebar-item ${selectedObject === obj.name ? "active" : ""}`}
                            onClick={() => onSelectObject(obj.name)}
                        >
                            <div className="sidebar-item-title">{obj.name}</div>
                            <div className="sidebar-item-subtitle">{obj.type}</div>
                        </button>
                    ))
                )}
            </div>
        </div>
    );
}