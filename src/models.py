"""
Data models for the ETL pipeline.

These Pydantic models serve as data contracts between pipeline stages,
ensuring type safety and validation at every boundary.
"""

from datetime import datetime, date
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class OrderStatus(str, Enum):
    """Valid order status values."""
    PLACED = "placed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Order(BaseModel):
    """Represents a single order from the PostgreSQL source table.

    Used during the Extract stage to validate raw database rows
    before passing them to the Transform stage.
    """

    order_id: str
    restaurant_id: str
    user_id: str
    status: OrderStatus
    amount: Decimal = Field(ge=0)
    country: str = Field(min_length=2, max_length=2)
    created_at: datetime
    updated_at: datetime

    @field_validator("country")
    @classmethod
    def country_uppercase(cls, v: str) -> str:
        return v.upper()

    @property
    def order_date(self) -> date:
        """The reporting date for this order (UTC date of creation)."""
        return self.created_at.date()

    @property
    def group_key(self) -> tuple[str, date, str]:
        """The aggregation group key: (restaurant_id, date, country)."""
        return (self.restaurant_id, self.order_date, self.country)


class DailyMetric(BaseModel):
    """Aggregated daily metrics for one restaurant-day-country group.

    This is the output of the Transform stage and the input to the Load stage.
    Each instance corresponds to one document in the MongoDB daily_metrics collection.
    """

    restaurant_id: str
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    country: str = Field(min_length=2, max_length=2)
    total_orders: int = Field(ge=0)
    completed_orders: int = Field(ge=0)
    cancelled_orders: int = Field(ge=0)
    gross_revenue: Decimal = Field(ge=0)
    last_updated_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("country")
    @classmethod
    def country_uppercase(cls, v: str) -> str:
        return v.upper()

    def validate_consistency(self) -> bool:
        """Verify that metric counts are logically consistent.

        total_orders >= completed_orders + cancelled_orders
        (remaining are 'placed' orders still in progress)
        """
        return self.total_orders >= self.completed_orders + self.cancelled_orders

    def to_mongo_filter(self) -> dict:
        """Generate the MongoDB filter for upsert operations."""
        return {
            "restaurant_id": self.restaurant_id,
            "date": self.date,
            "country": self.country,
        }

    def to_mongo_update(self) -> dict:
        """Generate the MongoDB $set payload for upsert operations."""
        return {
            "$set": {
                "restaurant_id": self.restaurant_id,
                "date": self.date,
                "country": self.country,
                "total_orders": self.total_orders,
                "completed_orders": self.completed_orders,
                "cancelled_orders": self.cancelled_orders,
                "gross_revenue": float(self.gross_revenue),
                "last_updated_at": self.last_updated_at,
            }
        }
