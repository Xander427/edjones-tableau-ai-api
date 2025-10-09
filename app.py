from pydantic import BaseModel
from fastapi import FastAPI

app = FastAPI()

class AskRequest(BaseModel):
    question: str

@app.get("/")
def healthcheck():
    return {"status": "ok"}

@app.post("/ask")
async def ask(payload: AskRequest):
    return {"answer": f"You asked: {payload.question}"}
