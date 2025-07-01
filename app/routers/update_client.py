from fastapi import APIRouter
from app.config import get_config
router = APIRouter()

@router.get("/update_client")
def update_client() -> dict[str, str|bool]:
    return {
        "update_client_url": get_config().mcsd.update_client_url,
        "authentication": get_config().mcsd.authentication,
    }
