from fastapi import FastAPI, Request

app = FastAPI()

@app.get("/")
def healthcheck():
    return {"status": "ok"}

@app.post("/ask")
async def ask(request: Request, question: str):
    return {"answer": f"You asked: {question}"}
