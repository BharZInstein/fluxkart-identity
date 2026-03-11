# fluxkart-identity

REST microservice that reconciles fragmented customer identities across transactions.

## Live Endpoint
https://fluxkart-identity-vxtq.onrender.com/identify

> Note: Free tier on Render — first request after inactivity may take ~50 seconds.

## POST /identify

**Request:**
```json
{
  "email": "string",
  "phoneNumber": "string"
}
```

**Response:**
```json
{
  "contact": {
    "primaryContatctId": 1,
    "emails": ["primary@email.com", "secondary@email.com"],
    "phoneNumbers": ["9876543210"],
    "secondaryContactIds": [2]
  }
}
```

## Stack
- FastAPI
- SQLite + SQLAlchemy
- Deployed on Render
