import {
    Columns3,
    Database,
    Gauge,
    GitBranch,
    LogOut,
    PanelLeftClose,
    PanelLeftOpen,
    Shield,
    Sparkles,
} from "lucide-react";
import { useAppContext } from "../context/AppContext";
import { useAuth } from "../context/AuthContext";

const NAV_GROUPS = [
    {
        label: "Engineering",
        items: [
            { key: "explorer", label: "Explorer", icon: Database },
            { key: "lineage", label: "ETL / Lineage", icon: GitBranch, roles: ["admin", "developer"] },
            { key: "insights", label: "Insights", icon: Sparkles },
        ],
    },
    {
        label: "Utilities",
        items: [
            { key: "comparator", label: "Env Comparator", icon: Columns3 },
            { key: "optimizer", label: "Optimizer", icon: Gauge },
        ],
    },
    {
        label: "Admin",
        items: [
            { key: "admin", label: "User Management", icon: Shield, roles: ["admin"] },
        ],
    },
];

export default function SidebarNav({
    activeItem,
    onSelect,
    collapsed,
    onToggleCollapse,
    userRole,
}) {
    const { clearAllCachedState } = useAppContext();
    const { logout } = useAuth();

    const handleLogout = () => {
        clearAllCachedState();
        logout();
        onSelect("explorer");
    };

    // Filter nav groups & items based on role
    const visibleGroups = NAV_GROUPS.map((group) => ({
        ...group,
        items: group.items.filter(
            (item) => !item.roles || item.roles.includes(userRole)
        ),
    })).filter((group) => group.items.length > 0);

    return (
        <aside className={`app-sidebar ${collapsed ? "collapsed" : ""}`}>
            <div className="sidebar-brand">
                <div className="sidebar-brand-left">
                    <div className="sidebar-brand-logo">DC</div>

                    {!collapsed && (
                        <div>
                            <div className="sidebar-brand-title">Data Copilot</div>
                            <div className="sidebar-brand-subtitle">Lifecycle Platform</div>
                        </div>
                    )}
                </div>
            </div>

            <div className="sidebar-collapse-row">
                <button
                    type="button"
                    className="icon-btn sidebar-toggle-btn"
                    onClick={onToggleCollapse}
                    title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
                    aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
                >
                    {collapsed ? <PanelLeftOpen size={18} /> : <PanelLeftClose size={18} />}
                </button>
            </div>

            <nav className="sidebar-nav-groups">
                {visibleGroups.map((group) => (
                    <div className="sidebar-group" key={group.label}>
                        {!collapsed && <div className="sidebar-group-label">{group.label}</div>}

                        <div className="sidebar-group-items">
                            {group.items.map((item) => {
                                const Icon = item.icon;

                                return (
                                    <button
                                        key={item.key}
                                        type="button"
                                        className={`sidebar-nav-item ${activeItem === item.key ? "active" : ""}`}
                                        onClick={() => onSelect(item.key)}
                                        title={collapsed ? item.label : ""}
                                        aria-label={item.label}
                                    >
                                        <span className="sidebar-nav-icon">
                                            <Icon size={18} />
                                        </span>
                                        {!collapsed && <span className="sidebar-nav-label">{item.label}</span>}
                                    </button>
                                );
                            })}
                        </div>
                    </div>
                ))}
            </nav>

            <div className="sidebar-footer">
                <button
                    type="button"
                    className="sidebar-logout-btn"
                    onClick={handleLogout}
                    title={collapsed ? "Logout" : ""}
                    aria-label="Logout"
                >
                    <span className="sidebar-nav-icon">
                        <LogOut size={18} />
                    </span>
                    {!collapsed && <span className="sidebar-nav-label">Logout</span>}
                </button>
            </div>
        </aside>
    );
}