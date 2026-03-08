import os

import uvicorn


if __name__ == "__main__":
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8000"))
    uvicorn.run("sql_monitor.web.app:app", host=host, port=port)
