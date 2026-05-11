"""TapeSessionService — an ADK `BaseSessionService` backed by the Tape server.

Why a custom session service rather than just a plugin? Because this is the seam
that gives single-transaction atomicity: routing ADK's `append_event` through
Tape lets the server commit "the ADK event + the session state delta + the tape
projection (decision / effect / obligation) this event corresponds to" together.
The event stream and the journal never disagree.

Event (de)serialization uses ADK's own Pydantic round-trip: the full `Event` is
stored as JSON in the record's `content_json` field, so nothing is lost.
"""

from __future__ import annotations

import json
from typing import Optional

from google.adk.sessions.base_session_service import (
    BaseSessionService,
    GetSessionConfig,
    ListSessionsResponse,
)
from google.adk.sessions.session import Session
from google.adk.events.event import Event

from ..client import TapeClient, DEFAULT_URL
from .._gen import tape_pb2 as pb


def _event_to_proto(event: Event) -> pb.EventRecord:
    return pb.EventRecord(
        id=event.id or "",
        invocation_id=event.invocation_id or "",
        author=event.author or "",
        branch=event.branch or "",
        content_json=event.model_dump_json(exclude_none=True),
        actions_json="",
        timestamp_ms=int((event.timestamp or 0) * 1000),
    )


def _proto_to_event(rec: pb.EventRecord) -> Event:
    if rec.content_json:
        return Event.model_validate_json(rec.content_json)
    # Fallback for records written by a non-Python SDK.
    return Event(id=rec.id or None, invocation_id=rec.invocation_id, author=rec.author or "user",
                 branch=rec.branch or None, timestamp=(rec.timestamp_ms / 1000.0) if rec.timestamp_ms else None)


def _filter_persistable_delta(delta: Optional[dict]) -> dict:
    if not delta:
        return {}
    return {k: v for k, v in delta.items() if not k.startswith("temp:")}


class TapeSessionService(BaseSessionService):
    def __init__(self, url: str = DEFAULT_URL, *, client: Optional[TapeClient] = None):
        self._client = client or TapeClient(url)

    async def create_session(self, *, app_name: str, user_id: str,
                             state: Optional[dict] = None, session_id: Optional[str] = None) -> Session:
        resp = self._client.create_session(
            app_name=app_name, user_id=user_id,
            session_id=session_id or "", state_json=json.dumps(state or {}))
        return Session(
            id=resp.session_id, app_name=app_name, user_id=user_id,
            state=json.loads(resp.state_json or "{}"), events=[],
            last_update_time=(resp.last_update_time_ms / 1000.0))

    async def get_session(self, *, app_name: str, user_id: str, session_id: str,
                          config: Optional[GetSessionConfig] = None) -> Optional[Session]:
        max_events = 0
        if config is not None and getattr(config, "num_recent_events", None):
            max_events = config.num_recent_events
        resp = self._client.get_session(app_name=app_name, user_id=user_id,
                                        session_id=session_id, max_events=max_events)
        if not resp.found:
            return None
        s = resp.session
        events = [_proto_to_event(e) for e in s.events]
        sess = Session(
            id=session_id, app_name=app_name, user_id=user_id,
            state=json.loads(s.state_json or "{}"), events=events,
            last_update_time=(s.last_update_time_ms / 1000.0))
        return sess

    async def list_sessions(self, *, app_name: str, user_id: Optional[str] = None) -> ListSessionsResponse:
        resp = self._client.list_sessions(app_name=app_name, user_id=user_id or "")
        sessions = [
            Session(id=s.session_id, app_name=app_name, user_id=(s.user_id or user_id or ""),
                    state=json.loads(s.state_json or "{}"), events=[],
                    last_update_time=(s.last_update_time_ms / 1000.0))
            for s in resp.sessions
        ]
        return ListSessionsResponse(sessions=sessions)

    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        self._client.delete_session(app_name=app_name, user_id=user_id, session_id=session_id)

    async def append_event(self, session: Session, event: Event) -> Event:
        # 1. Let the base class update the in-memory session (appends the event,
        #    applies the state delta, bumps last_update_time, filters temp:).
        event = await super().append_event(session=session, event=event)
        # 2. Persist to Tape. Partial (streaming) events are not committed.
        if not getattr(event, "partial", False):
            delta = _filter_persistable_delta(
                getattr(event.actions, "state_delta", None) if event.actions else None)
            self._client.append_event(
                app_name=session.app_name, user_id=session.user_id, session_id=session.id,
                event=_event_to_proto(event), state_delta_json=json.dumps(delta))
        return event
