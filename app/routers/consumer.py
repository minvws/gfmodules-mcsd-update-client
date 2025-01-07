from fastapi import APIRouter
from app.config import get_config
router = APIRouter()

@router.get("/consumer")
def consumer() -> dict[str, str|bool]:
    return {
        "consumer_url": get_config().mcsd.consumer_url,
        "authentication": get_config().mcsd.authentication,
    }
