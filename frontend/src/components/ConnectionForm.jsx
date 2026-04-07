import { useEffect, useMemo, useState } from "react";
import { saveConnection, testConnection, fetchSavedConnections } from "../api/schemaApi";
import { useAppContext } from "../context/AppContext";

const INITIAL_FORM = {
  name: "Local Postgres",
  db_type: "postgres",
  host: "localhost",
  port: "5433",
  database_name: "dlcopilot",
  schema_name: "public",
  username: "postgres",
  password: "",
  account: "",
  warehouse: "",
  role: "",
};

export default function ConnectionForm({ onConnectionSaved }) {
  const {
    connectionPayload,
    setConnectionPayload,
    sessionPassword,
    setSessionPassword,
  } = useAppContext();

  const [form, setForm] = useState({
    ...INITIAL_FORM,
    ...connectionPayload,
    password: sessionPassword || "",
  });

  const [testing, setTesting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [existingConnections, setExistingConnections] = useState([]);
  const [loadingConnections, setLoadingConnections] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoadingConnections(true);
    fetchSavedConnections()
      .then((data) => {
        if (!cancelled) setExistingConnections(Array.isArray(data) ? data : []);
      })
      .catch(() => { })
      .finally(() => { if (!cancelled) setLoadingConnections(false); });
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    setForm((prev) => ({
      ...prev,
      ...connectionPayload,
      password: sessionPassword || prev.password || "",
    }));
  }, [connectionPayload, sessionPassword]);

  const isPostgres = form.db_type === "postgres";
  const isMySQL = form.db_type === "mysql";
  const isSnowflake = form.db_type === "snowflake";

  const helpText = useMemo(() => {
    if (isPostgres) {
      return "Enter PostgreSQL server details. Values are cached in the browser, and the password is kept only for this browser session.";
    }
    if (isMySQL) {
      return "Enter MySQL server details. Database selection can be done after connection.";
    }
    if (isSnowflake) {
      return "Enter Snowflake account details such as account, warehouse, database, schema, username, password, and optional role.";
    }
    return "";
  }, [isPostgres, isMySQL, isSnowflake]);

  const persistForm = (nextForm) => {
    const { password, ...safePayload } = nextForm;
    setConnectionPayload(safePayload);
    setSessionPassword(password || "");
  };

  const updateField = (key, value) => {
    setMessage("");
    setError("");

    setForm((prev) => {
      const next = { ...prev, [key]: value };
      persistForm(next);
      return next;
    });
  };

  const handleDbTypeChange = (value) => {
    setMessage("");
    setError("");

    let nextForm;

    if (value === "postgres") {
      nextForm = {
        ...INITIAL_FORM,
        db_type: "postgres",
      };
    } else if (value === "mysql") {
      nextForm = {
        ...INITIAL_FORM,
        name: "Local MySQL",
        db_type: "mysql",
        host: "localhost",
        port: "3306",
        database_name: "",
        schema_name: "",
        username: "root",
        password: "",
        account: "",
        warehouse: "",
        role: "",
      };
    } else {
      nextForm = {
        ...INITIAL_FORM,
        name: "Snowflake Connection",
        db_type: "snowflake",
        host: "",
        port: "",
        database_name: "",
        schema_name: "",
        username: "",
        password: "",
        account: "",
        warehouse: "",
        role: "",
      };
    }

    setForm(nextForm);
    persistForm(nextForm);
  };

  const buildPayload = () => ({
    name: form.name,
    db_type: form.db_type,
    host: form.host,
    port: form.port,
    database_name: form.database_name,
    schema_name: form.schema_name,
    username: form.username,
    password: form.password,
    account: form.account,
    warehouse: form.warehouse,
    role: form.role,
  });

  const handleTest = async () => {
    setTesting(true);
    setMessage("");
    setError("");

    try {
      const payload = buildPayload();
      const res = await testConnection(payload);
      persistForm(payload);
      setMessage(res.message || "Connection successful");
    } catch (err) {
      setError(err.message || "Connection failed");
    } finally {
      setTesting(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    setMessage("");
    setError("");

    try {
      const payload = buildPayload();
      const res = await saveConnection(payload);
      persistForm(payload);
      setMessage("Connection saved successfully");
      onConnectionSaved?.(res.id, payload);
    } catch (err) {
      setError(err.message || "Failed to save connection");
    } finally {
      setSaving(false);
    }
  };

  const handleSelectExisting = (connId) => {
    const selected = existingConnections.find((c) => c.id === Number(connId));
    if (!selected) return;

    const nextForm = {
      name: selected.name || "",
      db_type: selected.db_type || "postgres",
      host: selected.host || "localhost",
      port: selected.port || "",
      database_name: selected.database_name || "",
      schema_name: selected.schema_name || "",
      username: selected.username || "",
      password: form.password || "",
      account: selected.account || "",
      warehouse: selected.warehouse || "",
      role: selected.role || "",
    };
    setForm(nextForm);
    persistForm(nextForm);
    onConnectionSaved?.(selected.id, nextForm);
    setMessage(`Connected using saved connection "${selected.name}"`);
    setError("");
  };

  return (
    <div className="connection-card">
      <div className="connection-header">
        <div>
          <h2>Database Connection</h2>
          <p>{helpText}</p>
        </div>
      </div>

      {existingConnections.length > 0 && (
        <div className="saved-connections-section">
          <div className="form-field">
            <label>Use a Saved Connection</label>
            <select
              defaultValue=""
              onChange={(e) => e.target.value && handleSelectExisting(e.target.value)}
              disabled={loadingConnections}
            >
              <option value="">
                {loadingConnections ? "Loading..." : "-- Select an existing connection --"}
              </option>
              {existingConnections.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name} ({c.db_type} — {c.host}:{c.port}/{c.database_name})
                </option>
              ))}
            </select>
          </div>
          <div className="saved-connections-divider">
            <span>or create a new connection below</span>
          </div>
        </div>
      )}

      <div className="connection-grid">
        <div className="form-field">
          <label>Connection Name</label>
          <input
            type="text"
            value={form.name}
            onChange={(e) => updateField("name", e.target.value)}
            placeholder="My local database"
          />
        </div>

        <div className="form-field">
          <label>Database Type</label>
          <select
            value={form.db_type}
            onChange={(e) => handleDbTypeChange(e.target.value)}
          >
            <option value="postgres">PostgreSQL</option>
            <option value="mysql">MySQL</option>
            <option value="snowflake">Snowflake</option>
          </select>
        </div>

        {(isPostgres || isMySQL) && (
          <>
            <div className="form-field">
              <label>Host</label>
              <input
                type="text"
                value={form.host}
                onChange={(e) => updateField("host", e.target.value)}
                placeholder={isPostgres ? "localhost" : "127.0.0.1"}
              />
            </div>

            <div className="form-field">
              <label>Port</label>
              <input
                type="text"
                value={form.port}
                onChange={(e) => updateField("port", e.target.value)}
                placeholder={isPostgres ? "5432" : "3306"}
              />
            </div>

            <div className="form-field">
              <label>Default Database</label>
              <input
                type="text"
                value={form.database_name}
                onChange={(e) => updateField("database_name", e.target.value)}
                placeholder={isPostgres ? "postgres or dlcopilot" : "my_database"}
              />
            </div>

            <div className="form-field">
              <label>Default Schema</label>
              <input
                type="text"
                value={form.schema_name}
                onChange={(e) => updateField("schema_name", e.target.value)}
                placeholder={isPostgres ? "public" : "optional"}
              />
            </div>

            <div className="form-field">
              <label>Username</label>
              <input
                type="text"
                value={form.username}
                onChange={(e) => updateField("username", e.target.value)}
                placeholder={isPostgres ? "postgres" : "root"}
              />
            </div>

            <div className="form-field">
              <label>Password</label>
              <input
                type="password"
                value={form.password}
                onChange={(e) => updateField("password", e.target.value)}
                placeholder="Enter password"
              />
            </div>
          </>
        )}

        {isSnowflake && (
          <>
            <div className="form-field">
              <label>Account</label>
              <input
                type="text"
                value={form.account}
                onChange={(e) => updateField("account", e.target.value)}
                placeholder="xy12345.ap-south-1"
              />
            </div>

            <div className="form-field">
              <label>Warehouse</label>
              <input
                type="text"
                value={form.warehouse}
                onChange={(e) => updateField("warehouse", e.target.value)}
                placeholder="COMPUTE_WH"
              />
            </div>

            <div className="form-field">
              <label>Database</label>
              <input
                type="text"
                value={form.database_name}
                onChange={(e) => updateField("database_name", e.target.value)}
                placeholder="MY_DATABASE"
              />
            </div>

            <div className="form-field">
              <label>Schema</label>
              <input
                type="text"
                value={form.schema_name}
                onChange={(e) => updateField("schema_name", e.target.value)}
                placeholder="PUBLIC"
              />
            </div>

            <div className="form-field">
              <label>Username</label>
              <input
                type="text"
                value={form.username}
                onChange={(e) => updateField("username", e.target.value)}
                placeholder="my_user"
              />
            </div>

            <div className="form-field">
              <label>Password</label>
              <input
                type="password"
                value={form.password}
                onChange={(e) => updateField("password", e.target.value)}
                placeholder="Enter password"
              />
            </div>

            <div className="form-field">
              <label>Role</label>
              <input
                type="text"
                value={form.role}
                onChange={(e) => updateField("role", e.target.value)}
                placeholder="Optional role"
              />
            </div>
          </>
        )}
      </div>

      <div className="connection-actions">
        <button onClick={handleTest} disabled={testing}>
          {testing ? "Testing..." : "Test Connection"}
        </button>
        <button onClick={handleSave} disabled={saving}>
          {saving ? "Saving..." : "Save Connection"}
        </button>
      </div>

      {message ? <div className="form-success">{message}</div> : null}
      {error ? <div className="form-error">{error}</div> : null}
    </div>
  );
}