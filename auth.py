#auth.py
import jwt, time
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from repositories import repo
import os
from dotenv import load_dotenv
load_dotenv() 

SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"
TOKEN_EXP_SECONDS = int(os.environ["TOKEN_EXP_SECONDS"])

security = HTTPBearer()

def create_token(user):
    payload = {
        "sub": user["id"],
        "username": user["username"],
        "exp": int(time.time()) + TOKEN_EXP_SECONDS
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)



def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(
            credentials.credentials,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )

        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(401, "Invalid token")

        user = repo.get_user_by_id(user_id)
        if not user:
            raise HTTPException(401, "User not found")

        return user

    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")


