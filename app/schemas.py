from typing import Any, Literal

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=255, examples=["evt_9f2c1a"])
    type: str = Field(min_length=1, max_length=255, examples=["payment.succeeded"])
    payload: Any = Field(examples=[{"order_id": "o_123", "amount": 9900}])


class EventOut(BaseModel):
    event_id: str
    type: str
    payload: Any
    status: Literal["pending", "acknowledged"]
    received_at: str
    acked_at: str | None = None


class EventPage(BaseModel):
    items: list[EventOut]
    next_cursor: str | None = None
