from fastapi import APIRouter
from app.config import get_config
router = APIRouter()

@router.get("/consumer")
def consumer() -> dict[str, str]:
    return {"consumer_url": get_config().mcsd.consumer_url}
