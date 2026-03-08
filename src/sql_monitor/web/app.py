from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from .auth import require_dashboard_auth
from .service import get_dashboard_snapshot, get_dashboard_timeseries


app = FastAPI(title="SQL Monitor Web")
static_dir = Path(__file__).resolve().parent / "static"
static_dir_resolved = static_dir.resolve()


@app.get("/")
def index(_=Depends(require_dashboard_auth)):
    return FileResponse(static_dir / "index.html")


@app.get("/static/{asset_path:path}")
def static_asset(asset_path: str, _=Depends(require_dashboard_auth)):
    file_path = (static_dir / asset_path).resolve()
    if static_dir_resolved not in file_path.parents or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(file_path)


@app.get("/api/dashboard")
def dashboard(_=Depends(require_dashboard_auth)):
    return get_dashboard_snapshot()


@app.get("/api/timeseries")
def timeseries(
    window_minutes: int = Query(default=6 * 60, ge=15, le=7 * 24 * 60),
    bucket_minutes: int = Query(default=5, ge=1, le=60),
    _=Depends(require_dashboard_auth),
):
    return get_dashboard_timeseries(window_minutes=window_minutes, bucket_minutes=bucket_minutes)


@app.get("/health")
def health():
    snapshot = get_dashboard_snapshot()
    overall = snapshot.get("overall_status", "unknown")
    return {
        "status": "ok" if overall == "ok" else "degraded",
        "overall_status": overall,
        "generated_at": snapshot.get("generated_at"),
    }
