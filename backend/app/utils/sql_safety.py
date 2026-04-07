import re
from fastapi import HTTPException

BLOCKED_KEYWORDS = {
    "insert", "update", "delete", "drop", "alter", "truncate",
    "merge", "call", "grant", "revoke", "create", "replace"
}


def validate_read_only_sql(sql: str) -> None:
    normalized = re.sub(r"\s+", " ", sql.strip().lower())

    if ";" in normalized:
        raise HTTPException(status_code=400, detail="Multiple statements are not allowed.")

    if not (normalized.startswith("select") or normalized.startswith("with")):
        raise HTTPException(status_code=400, detail="Only read-only SELECT queries are allowed.")

    for keyword in BLOCKED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", normalized):
            raise HTTPException(status_code=400, detail=f"Blocked SQL keyword detected: {keyword}")