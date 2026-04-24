"""
models.py
---------
Definisi schema/struktur data Event menggunakan Pydantic.

Setiap event yang dikirim publisher WAJIB memiliki field:
- topic     : kategori event, contoh "app-logs", "error-logs"
- event_id  : ID unik event, gunakan UUID v4
- timestamp : waktu event dalam format ISO 8601
- source    : nama service/aplikasi pengirim
- payload   : data tambahan bebas (boleh kosong)
"""

from pydantic import BaseModel, field_validator
from typing import Any, Dict, List
from datetime import datetime


class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any] = {}

    @field_validator("topic")
    @classmethod
    def topic_tidak_boleh_kosong(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic tidak boleh kosong")
        return v.strip()

    @field_validator("event_id")
    @classmethod
    def event_id_tidak_boleh_kosong(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("event_id tidak boleh kosong")
        return v.strip()

    @field_validator("timestamp")
    @classmethod
    def timestamp_harus_iso8601(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                "timestamp harus format ISO 8601, "
                "contoh: 2024-01-01T10:00:00Z"
            )
        return v


class PublishRequest(BaseModel):
    """
    Body request untuk POST /publish.
    Bisa mengirim satu atau banyak event sekaligus (batch).

    Contoh body single event:
    {
        "events": [
            {
                "topic": "app-logs",
                "event_id": "abc-123",
                "timestamp": "2024-01-01T10:00:00Z",
                "source": "service-A",
                "payload": {"level": "INFO", "msg": "started"}
            }
        ]
    }
    """
    events: List[Event]