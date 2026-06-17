from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.core.security import require_role, encrypt_value, decrypt_value
from app.core.app_store import get_app_store_conn, AppStoreOperationalError
from app.schemas.auth import UserResponse, UserUpdate
from app.services.auth_service import AuthService
from app.models.connection import Connection
from pydantic import BaseModel

router = APIRouter(prefix="/admin", tags=["Admin"])

_admin_only = require_role("admin")


# ── User management ───────────────────────────────────────────────────────────

@router.get("/users", response_model=list[UserResponse])
def list_users(
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    return AuthService.list_users(db)


@router.put("/users/{user_id}", response_model=UserResponse)
def update_user(
    user_id: int,
    payload: UserUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    return AuthService.update_user(db, user_id, payload)


@router.delete("/users/{user_id}", response_model=UserResponse)
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    return AuthService.delete_user(db, user_id)


# ── Connection schemas ────────────────────────────────────────────────────────

class AdminConnectionCreate(BaseModel):
    name: str
    db_type: str
    host: Optional[str] = None
    port: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    account: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None


class AdminConnectionResponse(BaseModel):
    id: int
    name: str
    db_type: str
    host: Optional[str] = None
    port: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    account: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    is_active: bool

    class Config:
        from_attributes = True


def _connection_signature(data: dict) -> str:
    return "|".join([
        str(data.get("name") or "").strip().lower(),
        str(data.get("db_type") or "").strip().lower(),
        str(data.get("host") or "").strip().lower(),
        str(data.get("port") or "").strip(),
        str(data.get("database_name") or "").strip().lower(),
        str(data.get("schema_name") or "").strip().lower(),
        str(data.get("username") or "").strip().lower(),
        str(data.get("account") or "").strip().lower(),
        str(data.get("warehouse") or "").strip().lower(),
        str(data.get("role") or "").strip().lower(),
    ])


def _dedupe_connection_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []

    # Keep newest rows for duplicate signatures (rows are ordered by id ascending).
    for row in reversed(rows):
        sig = _connection_signature(row)
        if sig in seen:
            continue
        seen.add(sig)
        out.append(row)

    out.reverse()
    return out


def _app_store_list_connections(active_only: bool = True):
    conn = get_app_store_conn()
    cur = conn.cursor()
    try:
        if active_only:
            cur.execute("SELECT * FROM connections WHERE is_active = TRUE ORDER BY id")
        else:
            cur.execute("SELECT * FROM connections ORDER BY id")
        rows = [dict(r) for r in cur.fetchall()]
    except AppStoreOperationalError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load connections: {exc}")
    finally:
        conn.close()

    rows = _dedupe_connection_rows(rows)

    for row in rows:
        row.pop("password", None)
        row["is_active"] = bool(row.get("is_active", True))
    return rows


def _app_store_find_connection(connection_id: int):
    conn = get_app_store_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM connections WHERE id = ? AND is_active = TRUE", (connection_id,))
        row = cur.fetchone()
    except AppStoreOperationalError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load connection: {exc}")
    finally:
        conn.close()
    return dict(row) if row else None


def _ensure_sql_connection_row(db: Session, connection_id: int, owner_user_id: int, conn_data: dict):
    row = db.query(Connection).filter(Connection.id == connection_id).first()
    if not row:
        row = Connection(
            id=connection_id,
            owner_user_id=owner_user_id,
            name=conn_data.get("name"),
            db_type=conn_data.get("db_type"),
            host=conn_data.get("host"),
            port=conn_data.get("port"),
            database_name=conn_data.get("database_name"),
            schema_name=conn_data.get("schema_name"),
            username=conn_data.get("username"),
            password_encrypted=None,
            account=conn_data.get("account"),
            warehouse=conn_data.get("warehouse"),
            role=conn_data.get("role"),
            is_active=True,
        )
        db.add(row)
        return

    row.owner_user_id = owner_user_id
    row.name = conn_data.get("name")
    row.db_type = conn_data.get("db_type")
    row.host = conn_data.get("host")
    row.port = conn_data.get("port")
    row.database_name = conn_data.get("database_name")
    row.schema_name = conn_data.get("schema_name")
    row.username = conn_data.get("username")
    row.account = conn_data.get("account")
    row.warehouse = conn_data.get("warehouse")
    row.role = conn_data.get("role")
    row.is_active = True


class AssignConnectionPayload(BaseModel):
    connection_id: Optional[int] = None  # None = unassign


# ── Connection management (admin only) ───────────────────────────────────────

@router.get("/connections", response_model=list[AdminConnectionResponse])
def admin_list_connections(
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    """Return all active runtime connections from app_store; passwords never returned."""
    return _app_store_list_connections(active_only=True)


@router.post("/connections", response_model=AdminConnectionResponse, status_code=201)
def admin_create_connection(
    payload: AdminConnectionCreate,
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    """Create a new named runtime connection in app_store. Admin only."""
    existing = _app_store_list_connections(active_only=True)
    if any((c.get("name") or "").strip().lower() == payload.name.strip().lower() for c in existing):
        raise HTTPException(status_code=409, detail=f"A connection named '{payload.name}' already exists.")

    next_id = max([int(c.get("id", 0)) for c in existing], default=0) + 1

    conn = get_app_store_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO connections (
                id, name, db_type, host, port, database_name, schema_name,
                username, password, account, warehouse, role, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                next_id,
                payload.name,
                payload.db_type,
                payload.host,
                payload.port,
                payload.database_name,
                payload.schema_name,
                payload.username,
                payload.password or "",
                payload.account,
                payload.warehouse,
                payload.role,
                True,
            ),
        )
        conn.commit()
    except AppStoreOperationalError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create connection: {exc}")
    finally:
        conn.close()

    _ensure_sql_connection_row(
        db,
        next_id,
        current_user.id,
        {
            "name": payload.name,
            "db_type": payload.db_type,
            "host": payload.host,
            "port": payload.port,
            "database_name": payload.database_name,
            "schema_name": payload.schema_name,
            "username": payload.username,
            "account": payload.account,
            "warehouse": payload.warehouse,
            "role": payload.role,
        },
    )
    db.commit()

    return {
        "id": next_id,
        "name": payload.name,
        "db_type": payload.db_type,
        "host": payload.host,
        "port": payload.port,
        "database_name": payload.database_name,
        "schema_name": payload.schema_name,
        "username": payload.username,
        "account": payload.account,
        "warehouse": payload.warehouse,
        "role": payload.role,
        "is_active": True,
    }


@router.delete("/connections/{conn_id}", status_code=204)
def admin_delete_connection(
    conn_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    """Soft-delete a runtime connection. Clears it from any assigned users first."""
    existing = _app_store_find_connection(conn_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Unassign from all users
    from app.models.user import User
    db.query(User).filter(User.connection_id == conn_id).update({"connection_id": None})

    store_conn = get_app_store_conn()
    cur = store_conn.cursor()
    try:
        cur.execute("UPDATE connections SET is_active = FALSE WHERE id = ?", (conn_id,))
        store_conn.commit()
    except AppStoreOperationalError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to delete connection: {exc}")
    finally:
        store_conn.close()

    sql_conn = db.query(Connection).filter(Connection.id == conn_id).first()
    if sql_conn:
        sql_conn.is_active = False

    db.commit()


# ── Assign connection to user ─────────────────────────────────────────────────

@router.put("/users/{user_id}/connection", response_model=UserResponse)
def assign_connection_to_user(
    user_id: int,
    payload: AssignConnectionPayload,
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    """Assign (or unassign) a connection to a specific user."""
    from app.models.user import User
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if payload.connection_id is not None:
        conn = _app_store_find_connection(payload.connection_id)
        if not conn:
            raise HTTPException(status_code=404, detail="Connection not found")
        _ensure_sql_connection_row(db, payload.connection_id, current_user.id, conn)

    user.connection_id = payload.connection_id
    db.commit()
    db.refresh(user)
    return user

