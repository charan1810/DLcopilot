from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.security import get_current_user
from app.schemas.query import QueryRequest, QueryResponse
from app.services.query_service import QueryService

router = APIRouter(prefix="/query", tags=["Query"])


@router.post("/run", response_model=QueryResponse)
def run_query(payload: QueryRequest, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    result = QueryService.run_query(
        db=db,
        connection_id=payload.connection_id,
        sql=payload.sql,
        limit=payload.limit,
    )
    return result