import logging

from fastapi import APIRouter, HTTPException, Depends

from app import container
from app.db.db import Database
from app.db.models import ExampleResponse, Meta, Example

logger = logging.getLogger(__name__)
router = APIRouter()

PAGE_LIMIT = 25


@router.get("/examples",
            summary="Search for examples based on the query parameter",
            tags=["example_group"]
            )
def get_examples(q: str, db: Database = Depends(container.get_database)) -> ExampleResponse:
    """
    Returns a list of examples based on the query parameter
    """
    if len(q) == 0:
        raise HTTPException(status_code=400, detail="Missing or empty query parameter")

    with db.get_db_session() as session:
        repository = session.get_repository(Example)
        (total, result) = repository.find_example(q, offset=0, limit=PAGE_LIMIT)
        items = list(map(lambda example: example.name, result))

        return ExampleResponse(meta=Meta(offset=0, limit=len(items), total=total), items=items)


@router.get("/examples/all",
            summary="Get all examples as a single JSON response",
            tags=["example_group"]
            )
def get_all_examples(db: Database = Depends(container.get_database)) -> ExampleResponse:
    """
    Returns all examples as a single JSON response
    """
    with db.get_db_session() as session:
        repository = session.get_repository(Example)
        (total, result) = repository.find_all_examples()

        items = list(map(lambda example: example.name, result))

        return ExampleResponse(meta=Meta(offset=0, limit=len(items), total=total), items=items)
