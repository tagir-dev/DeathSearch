from sqlalchemy import create_engine, Column, Integer, String,select
from sqlalchemy.orm import Session, declarative_base
from typing import Optional

import config
import uvicorn

from fastapi import FastAPI,status
from fastapi.responses import JSONResponse

database_Url = f"postgresql://{config.user}:{config.password}@{config.host}:5432/{config.database}"
engine = create_engine(database_Url)

session = Session(engine, future=True)

Base = declarative_base()

class Deadth_stat(Base):
    __tablename__ = 'mortality_data'
    id = Column(Integer, primary_key=True)
    country = Column(String)
    year = Column(String)
    cause = Column(String)
    deaths = Column(Integer)

app = FastAPI()

@app.get("/search_mortality_data")
def search_mortality_data(
    country: Optional[str] = None,
    year: Optional[int] = None,
    cause: Optional[str] = None,
    limit: int = 10,
    offset: int = 0
) -> list:
    query = select(Deadth_stat)

    if country:
        query = query.where(Deadth_stat.country.ilike(f"%{country}%"))

    if year:
        query = query.where(Deadth_stat.year == str(year))

    if cause:
        query = query.where(Deadth_stat.cause.ilike(f"%{cause}%"))

    query = query.limit(limit).offset(offset)
    results = session.execute(query).scalars().all()
    data = []
    for obj in results:
        data.append({
            "id": obj.id,
            "country": obj.country,
            "year": obj.year,
            "cause": obj.cause,
            "deaths": obj.deaths
        })
    return JSONResponse(status_code=status.HTTP_200_OK, content=data)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    