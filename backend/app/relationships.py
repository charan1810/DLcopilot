import re

ID_PATTERN = re.compile(r"(?:^id$|_id$|key$|code$|ref$)", re.IGNORECASE)


def is_id_like(col_name: str) -> bool:
    return bool(ID_PATTERN.search(col_name or ""))


def compute_pk_score(col_name: str, stats: dict):
    score = 0
    reasons = []

    uniqueness = stats.get("uniqueness", 0)
    null_ratio = stats.get("null_ratio", 1)

    if uniqueness >= 0.99:
        score += 40
        reasons.append("high uniqueness")

    if null_ratio <= 0.01:
        score += 25
        reasons.append("mostly non-null")

    if is_id_like(col_name):
        score += 20
        reasons.append("id-like column name")

    if (col_name or "").lower() == "id":
        score += 15
        reasons.append("exact id column")

    return min(score / 100, 1.0), reasons


def compute_fk_score(src_col: str, tgt_col: str, src_stats: dict, tgt_stats: dict):
    score = 0
    reasons = []

    src_col_l = (src_col or "").lower()
    tgt_col_l = (tgt_col or "").lower()

    if src_col_l == tgt_col_l:
        score += 25
        reasons.append("same column name")

    if is_id_like(src_col) and is_id_like(tgt_col):
        score += 20
        reasons.append("id-like naming pattern")

    if src_stats.get("data_type") == tgt_stats.get("data_type"):
        score += 15
        reasons.append("same datatype")

    if tgt_stats.get("uniqueness", 0) >= 0.99:
        score += 20
        reasons.append("target column looks unique")

    if src_stats.get("uniqueness", 1) < 0.5:
        score += 10
        reasons.append("source column repeats values")

    return min(score / 100, 1.0), reasons