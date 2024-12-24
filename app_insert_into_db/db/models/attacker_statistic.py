from sqlalchemy import Column, Integer, Float, ForeignKey, String
from sqlalchemy.orm import relationship
from app_insert_into_db.db.models import Base


class AttackerStatistic(Base):
    __tablename__ = 'attackerstatistics'
    stat_id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey('events.event_id'))
    n_kill = Column(Float)
    n_wound = Column(Float)
    n_per_ps = Column(Float)
    n_kill_ter = Column(Float)
    n_wound_ter = Column(Float)
    summary = Column(String(250))

    event = relationship("Event", backref="attacker_statistics")
