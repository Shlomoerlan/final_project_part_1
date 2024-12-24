from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app_insert_into_db.db.models import Base
from app_insert_into_db.settings.config import DB_URL

engine = create_engine(DB_URL)
session_maker = sessionmaker(bind=engine)


def init_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(engine)

if __name__ == '__main__':
    init_db()
