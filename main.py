from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db, init_db
from schemas import IdentifyRequest, IdentifyResponse
from service import identify_contact, build_response

app = FastAPI()


@app.on_event("startup")
def startup():
    init_db()


@app.post("/identify", response_model=IdentifyResponse)
def identify(req: IdentifyRequest, db: Session = Depends(get_db)):
    if not req.email and not req.phoneNumber:
        raise HTTPException(status_code=400, detail="Either email or phoneNumber required")
    
    primary = identify_contact(db, req.email, req.phoneNumber)
    if not primary:
        raise HTTPException(status_code=500, detail="Failed to process contact")
    
    contact_data = build_response(db, primary)
    return IdentifyResponse(contact=contact_data)

