import logging as log

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from api.api.collect import collect


app = FastAPI(docs_url=None, redoc_url=None)


@app.get("/ping")
async def ping():
    return JSONResponse(content={"status": "OK"}, status_code=200)


@app.post("/collect")
async def collect_endpoint(request: Request):
    try:
        event = await request.json()
        result = collect(event)
        return JSONResponse(content=result, status_code=200)
    except Exception as e:
        log.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
