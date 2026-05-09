import { useEffect, useMemo, useRef, useState } from "react";
import SchemaExplorerPage from "./pages/SchemaExplorerPage";
import LineagePage from "./pages/LineagePage";
import InsightsPage from "./pages/InsightsPage";
import LoginPage from "./pages/LoginPage";
import SignupPage from "./pages/SignupPage";
import AdminUsersPage from "./pages/AdminUsersPage";
import EnvComparatorPage from "./pages/EnvComparatorPage";
import OptimizerPage from "./pages/OptimizerPage";
import TopHeader from "./components/TopHeader";
import SidebarNav from "./components/SidebarNav";
import AppErrorBoundary from "./components/AppErrorBoundary";
import { useAppContext } from "./context/AppContext";
import { useAuth } from "./context/AuthContext";
import "./styles.css";

function PlaceholderPage({ title, subtitle }) {
    return (
        <div className="module-shell">
            <div className="module-hero">
                <div>
                    <div className="module-badge">Coming Soon</div>
                    <h1 className="module-title">{title}</h1>
                    <p className="module-subtitle">{subtitle}</p>
                </div>
            </div>

            <div className="placeholder-grid">
                <div className="placeholder-card">
                    <h3>Planned Features</h3>
                    <p>
                        This module is part of the Data Copilot workflow and will be connected
                        with the active engineering context.
                    </p>
                </div>

                <div className="placeholder-card">
                    <h3>Context Awareness</h3>
                    <p>
                        Selected database, schema, and object can later be carried across
                        Explorer, ETL / Lineage, and Insights.
                    </p>
                </div>
            </div>
        </div>
    );
}

export default function App() {
    const { activePage, setActivePage, clearAllCachedState } = useAppContext();
    const { isAuthenticated, loading: authLoading, user } = useAuth();
    const [authView, setAuthView] = useState("login"); // "login" | "signup"
    const [theme, setTheme] = useState("light");
    const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
    const previousAuthenticatedRef = useRef(false);

    useEffect(() => {
        if (isAuthenticated && !previousAuthenticatedRef.current) {
            setActivePage("explorer");
        }
        previousAuthenticatedRef.current = isAuthenticated;
    }, [isAuthenticated, setActivePage]);

    useEffect(() => {
        if (!isAuthenticated) {
            return;
        }
        if (activePage === "admin" && user?.role !== "admin") {
            setActivePage("explorer");
        }
    }, [activePage, isAuthenticated, setActivePage, user?.role]);

    const activeMeta = useMemo(() => {
        const map = {
            explorer: {
                title: "Explorer",
                section: "Engineering",
            },
            lineage: {
                title: "ETL / Lineage",
                section: "Engineering",
            },
            insights: {
                title: "Insights",
                section: "Engineering",
            },
            comparator: {
                title: "Environment Comparator",
                section: "Utilities",
            },
            optimizer: {
                title: "Optimizer",
                section: "Utilities",
            },
            admin: {
                title: "User Management",
                section: "Admin",
            },
        };
        return map[activePage] || map.explorer;
    }, [activePage]);

    // ── Auth loading ──
    if (authLoading) {
        return (
            <div className={`auth-page theme-${theme}`}>
                <div className="auth-card" style={{ textAlign: "center" }}>
                    <div className="sidebar-brand-logo">DC</div>
                    <p style={{ marginTop: 16, opacity: 0.6 }}>Loading...</p>
                </div>
            </div>
        );
    }

    // ── Not authenticated ──
    if (!isAuthenticated) {
        return (
            <div className={`theme-${theme}`}>
                {authView === "login" ? (
                    <LoginPage onSwitchToSignup={() => setAuthView("signup")} />
                ) : (
                    <SignupPage onSwitchToLogin={() => setAuthView("login")} />
                )}
            </div>
        );
    }

    // ── Authenticated ──

    const effectiveActivePage = activePage === "admin" && user?.role !== "admin"
        ? "explorer"
        : activePage;

    const renderPage = () => {
        switch (effectiveActivePage) {
            case "explorer":
                return <SchemaExplorerPage />;
            case "lineage":
                return <LineagePage />;
            case "insights":
                return <InsightsPage />;
            case "comparator":
                return <EnvComparatorPage />;
            case "optimizer":
                return <OptimizerPage />;
            case "admin":
                return user?.role === "admin" ? (
                    <AdminUsersPage />
                ) : (
                    <PlaceholderPage title="Access Denied" subtitle="Admin privileges required." />
                );
            default:
                return <SchemaExplorerPage />;
        }
    };

    return (
        <div
            className={`app-shell theme-${theme} ${sidebarCollapsed ? "sidebar-collapsed" : ""}`}
        >
            <SidebarNav
                activeItem={effectiveActivePage}
                onSelect={setActivePage}
                collapsed={sidebarCollapsed}
                onToggleCollapse={() => setSidebarCollapsed((prev) => !prev)}
                userRole={user?.role}
            />

            <div className="app-main">
                <TopHeader
                    title={activeMeta.title}
                    section={activeMeta.section}
                    theme={theme}
                    onToggleTheme={() =>
                        setTheme((prev) => (prev === "light" ? "dark" : "light"))
                    }
                    sidebarCollapsed={sidebarCollapsed}
                    onToggleSidebar={() => setSidebarCollapsed((prev) => !prev)}
                />

                <AppErrorBoundary onReset={clearAllCachedState}>
                    <main className="app-content">{renderPage()}</main>
                </AppErrorBoundary>
            </div>
        </div>
    );
}