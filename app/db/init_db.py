from app.db.session import engine
from app.models.base import Base
from app.models import polling_job, price_point, raw_market_data, symbol_average  # noqa: F401


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
