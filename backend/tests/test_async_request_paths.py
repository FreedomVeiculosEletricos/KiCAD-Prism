"""Blocking stores must not occupy the event loop shared by every request."""

import asyncio
import threading
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi import HTTPException

from app.api import _helpers as api_helpers
from app.api import comments as comments_api
from app.core import security, session
from app.core.config import settings
from app.services import session_store_service

TEST_SECRET = "unit-test-session-secret-with-enough-length"


def _request(token: str):
    class _Request:
        cookies = {session.SESSION_COOKIE_NAME: token}
        headers: dict[str, str] = {}

    return _Request()


def _record(session_id: str, email: str) -> session_store_service.SessionRecord:
    now = datetime.now(timezone.utc)
    return session_store_service.SessionRecord(
        session_id=session_id,
        email=email,
        name="Designer",
        picture="",
        created_at=now,
        expires_at=now,
        last_seen_at=now,
    )


class _BlockedStore:
    """A store call that waits on a gate the test releases, mimicking a hung backend."""

    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.gate = threading.Event()
        self.entered = threading.Event()
        self.result = result
        self.error = error

    def __call__(self, *args, **kwargs):
        self.entered.set()
        if not self.gate.wait(timeout=5):
            raise AssertionError("store gate was never released")
        if self.error is not None:
            raise self.error
        return self.result


async def _lightweight_probe_completes_while(pending: asyncio.Task) -> bool:
    """Run a trivial coroutine next to `pending` and report whether it finished promptly."""
    probe = asyncio.create_task(asyncio.sleep(0.01))
    done, _ = await asyncio.wait({probe}, timeout=1.0)
    return probe in done and not pending.done()


class AuthenticationDoesNotBlockTheLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch.object(settings, "SESSION_SECRET", TEST_SECRET)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_blocked_session_store_does_not_stall_an_unrelated_request(self) -> None:
        token = session.create_session_token("slow-session")
        blocked = _BlockedStore()

        async def scenario() -> tuple[bool, HTTPException]:
            auth = asyncio.create_task(security.get_current_user(_request(token)))
            await asyncio.get_running_loop().run_in_executor(None, blocked.entered.wait, 2)
            probe_finished = await _lightweight_probe_completes_while(auth)
            blocked.gate.set()
            with self.assertRaises(HTTPException) as ctx:
                await auth
            return probe_finished, ctx.exception

        with patch.object(settings, "AUTH_ENABLED_OVERRIDE", True), patch.object(
            session_store_service, "load_session", blocked
        ):
            probe_finished, error = asyncio.run(scenario())

        self.assertTrue(probe_finished, "the event loop was blocked by the session store")
        # A store that answers "no session" still yields the revoked-session result.
        self.assertEqual(error.status_code, 401)

    def test_blocked_bearer_validation_does_not_stall_an_unrelated_request(self) -> None:
        blocked = _BlockedStore(error=HTTPException(status_code=401, detail="Invalid bearer token"))

        class _Request:
            cookies: dict[str, str] = {}
            headers = {"authorization": "Bearer v1.opaque-token"}

        async def scenario() -> bool:
            auth = asyncio.create_task(security.get_current_user(_Request()))
            await asyncio.get_running_loop().run_in_executor(None, blocked.entered.wait, 2)
            probe_finished = await _lightweight_probe_completes_while(auth)
            blocked.gate.set()
            with self.assertRaises(HTTPException):
                await auth
            return probe_finished

        with patch.object(settings, "AUTH_ENABLED_OVERRIDE", True), patch.object(
            security, "_resolve_bearer_user", blocked
        ):
            self.assertTrue(asyncio.run(scenario()))

    def test_revoked_and_unassigned_sessions_keep_their_results(self) -> None:
        token = session.create_session_token("session-a")
        with patch.object(settings, "AUTH_ENABLED_OVERRIDE", True):
            with patch.object(session_store_service, "load_session", return_value=None):
                with self.assertRaises(HTTPException) as revoked:
                    asyncio.run(security.get_current_user(_request(token)))
            with patch.object(
                session_store_service, "load_session", return_value=_record("session-a", "nobody@example.com")
            ), patch.object(security, "_resolve_allowed_user_role", return_value=None):
                with self.assertRaises(HTTPException) as unassigned:
                    asyncio.run(security.get_current_user(_request(token)))
            with patch.object(
                session_store_service, "load_session", return_value=_record("session-a", "qa@example.com")
            ), patch.object(security, "_resolve_allowed_user_role", return_value="qa"):
                user = asyncio.run(security.get_current_user(_request(token)))

        self.assertEqual(revoked.exception.status_code, 401)
        self.assertEqual(unassigned.exception.status_code, 403)
        self.assertEqual((user.email, user.role, user.session_id), ("qa@example.com", "qa", "session-a"))


class CommentReadsDoNotBlockTheLoopTests(unittest.TestCase):
    def test_blocked_comment_store_does_not_stall_an_unrelated_request(self) -> None:
        blocked = _BlockedStore()
        user = security.AuthenticatedUser(email="v@example.com", name="V", role="viewer")

        async def scenario() -> bool:
            read = asyncio.create_task(comments_api.get_comments("prj", user))
            await asyncio.get_running_loop().run_in_executor(None, blocked.entered.wait, 2)
            probe_finished = await _lightweight_probe_completes_while(read)
            blocked.gate.set()
            with self.assertRaises(HTTPException) as ctx:
                await read
            self.assertEqual(ctx.exception.status_code, 404)
            return probe_finished

        with patch.object(api_helpers.workspace, "get_project_for_role", blocked):
            self.assertTrue(asyncio.run(scenario()))

    def test_project_role_filtering_is_preserved_on_reads(self) -> None:
        user = security.AuthenticatedUser(email="v@example.com", name="V", role="viewer")
        with patch.object(api_helpers.workspace, "get_project_for_role", return_value=None) as lookup:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(comments_api.get_comments("prj", user))
        self.assertEqual(ctx.exception.status_code, 404)
        lookup.assert_called_once_with("prj", "viewer")


if __name__ == "__main__":
    unittest.main()
