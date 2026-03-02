from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import kpi_router
from routers import anomaly_router
from routers import ai_router

app = FastAPI(title="Supply Chain Control Tower")

app.include_router(kpi_router.router)
app.include_router(anomaly_router.router)
app.include_router(ai_router.router)

# Allow CORS for frontend
origins = [
    "http://localhost:5173",
    "http://localhost:3000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Supply Chain Control Tower API is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
