import asyncio
import os
from unittest.mock import AsyncMock, MagicMock

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from sqlalchemy.exc import IntegrityError

from app.repositories import SourceRepository


def test_source_create_returns_none_on_duplicate_name() -> None:
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock(side_effect=IntegrityError("insert", {"name": "dup"}, Exception("uniq")))
    session.rollback = AsyncMock()

    repo = SourceRepository(session)
    result = asyncio.run(repo.create(source_type="rss", name="dup", url="https://example.com"))

    assert result is None
    session.rollback.assert_awaited_once()
