import { useEffect, useRef, useState } from "react";
import { Moon, Sun } from "lucide-react";
import { useAuth } from "../context/AuthContext";

export default function TopHeader({
    title,
    section,
    theme,
    onToggleTheme,
    sidebarCollapsed: _sidebarCollapsed,
    onToggleSidebar: _onToggleSidebar,
}) {
    const { user, logout } = useAuth();
    const [isProfileOpen, setIsProfileOpen] = useState(false);
    const profileMenuRef = useRef(null);

    useEffect(() => {
        if (!isProfileOpen) return;

        const handleDocumentClick = (event) => {
            if (profileMenuRef.current && !profileMenuRef.current.contains(event.target)) {
                setIsProfileOpen(false);
            }
        };

        const handleEscape = (event) => {
            if (event.key === "Escape") {
                setIsProfileOpen(false);
            }
        };

        document.addEventListener("mousedown", handleDocumentClick);
        document.addEventListener("keydown", handleEscape);

        return () => {
            document.removeEventListener("mousedown", handleDocumentClick);
            document.removeEventListener("keydown", handleEscape);
        };
    }, [isProfileOpen]);

    const profileName = user?.full_name || "User";
    const profileEmail = user?.email || "";
    const profileRole = user?.role || "unknown";

    return (
        <header className="top-header">
            <div className="top-header-left">
                {/* <button
                    type="button"
                    className="icon-btn header-icon-btn"
                    onClick={onToggleSidebar}
                // title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                // aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                >
                    {sidebarCollapsed ? <PanelLeftOpen size={18} /> : <PanelLeftClose size={18} />}
                </button> */}

                {/* <div className="brand-mark">DC</div> */}

                <div>
                    {/* <div className="brand-name">Data Copilot</div> */}
                    <div className="brand-name">
                        {section} / {title}
                    </div>
                    {/* <div className="header-selection-context">
                        {selectedDatabase || "No DB"} • {selectedSchema || "No Schema"} •{" "}
                        {selectedObject || "No Object"}
                    </div> */}
                </div>
            </div>

            <div className="top-header-right">
                <button
                    className="icon-btn theme-icon-btn"
                    onClick={onToggleTheme}
                    type="button"
                    title={theme === "light" ? "Switch to dark theme" : "Switch to light theme"}
                    aria-label={theme === "light" ? "Switch to dark theme" : "Switch to light theme"}
                >
                    {theme === "light" ? <Moon size={18} /> : <Sun size={18} />}
                    <span className="icon-btn-text">
                        {theme === "light" ? "Dark" : "Light"}
                    </span>
                </button>

                <div className="profile-menu" ref={profileMenuRef}>
                    <button
                        className="profile-btn"
                        type="button"
                        onClick={() => setIsProfileOpen((prev) => !prev)}
                        aria-haspopup="menu"
                        aria-expanded={isProfileOpen}
                        title="Open profile"
                    >
                        Profile
                    </button>

                    {isProfileOpen ? (
                        <div className="profile-popover" role="menu" aria-label="User profile">
                            <div className="profile-popover-name">{profileName}</div>
                            {profileEmail ? (
                                <div className="profile-popover-email" title={profileEmail}>{profileEmail}</div>
                            ) : null}
                            <div className="profile-popover-role">Role: {profileRole}</div>
                            <button
                                type="button"
                                className="profile-logout-btn"
                                onClick={() => {
                                    setIsProfileOpen(false);
                                    logout();
                                }}
                            >
                                Log out
                            </button>
                        </div>
                    ) : null}
                </div>
            </div>
        </header>
    );
}