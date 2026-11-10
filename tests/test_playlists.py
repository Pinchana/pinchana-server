import asyncio
import json
import time
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from pinchana_server import main
from pinchana_server import playlists
from pinchana_server.playlists import (
    PlaylistJobs, PlaylistRequest, cached_asset, extract_collection, is_collection,
)
from pinchana_server.schemas import ScrapeV1Response


POSTS = [
    "https://www.tiktok.com/@user/video/123",
    "https://www.instagram.com/p/post/",
    "https://www.youtube.com/shorts/abcdefghijk",
    "https://soundcloud.com/artist/song",
    "https://music.youtube.com/watch?v=abcdefghijk",
    "https://open.spotify.com/track/abc123",
    "https://www.deezer.com/track/123",
    "https://www.threads.com/@user/post/123",
    "https://x.com/user/status/123",
    "https://coub.com/view/123",
]


def media_result(url=POSTS[1], media_type="video", filename="video.mp4"):
    return ScrapeV1Response(data={
        "id": "post", "source": {"platform": "instagram", "url": url},
        "content": {"title": "../Unsafe / Title"},
        "author": {"name": "Author"},
        "media": [{
            "index": 0, "type": media_type, "role": "content",
            "url": f"/media/instagram/post/{filename}",
        }],
    })


def create_media(tmp_path, filename="video.mp4"):
    directory = tmp_path / "post"
    directory.mkdir(exist_ok=True)
    (directory / filename).write_bytes(b"media")


@pytest.mark.parametrize("url", [
    "https://youtube.com/playlist?list=PLtest",
    "https://music.youtube.com/watch?v=abcdefghijk&list=PLtest",
    "https://soundcloud.com/artist/sets/name",
    "https://open.spotify.com/playlist/abc",
    "https://open.spotify.com/intl-uk/album/abc",
    "https://deezer.com/en/album/123",
    "https://www.tiktok.com/@user/collection/title-123",
])
def test_native_playlist_detection(url):
    assert is_collection(url)
    PlaylistRequest(url=url)


@pytest.mark.parametrize("url", [
    "https://localhost/playlist",
    "https://youtube.com.evil.example/playlist?list=test",
    "https://evil.example/?url=https://youtube.com/playlist?list=test",
    "https://user:password@youtube.com/playlist?list=test",
    "https://youtube.com:444/playlist?list=test",
    "http://youtube.com/playlist?list=test",
])
def test_untrusted_sources_are_rejected(url):
    with pytest.raises((HTTPException, ValidationError)):
        PlaylistRequest(url=url)


def test_custom_playlists_support_all_platforms_and_reject_nested_or_oversized_lists():
    assert len(PlaylistRequest(urls=POSTS).urls) == len(POSTS)
    for payload in [{}, {"url": "https://youtube.com/playlist?list=PLtest", "urls": POSTS},
                    {"urls": POSTS * 11}, {"urls": ["https://youtube.com/playlist?list=PLtest"]},
                    {"url": POSTS[1]}, {"urls": POSTS, "zip": False}]:
        with pytest.raises((HTTPException, ValidationError)):
            PlaylistRequest(**payload)


def test_zip_always_even_single_item_and_preserves_order(tmp_path):
    async def run():
        create_media(tmp_path)
        manager = PlaylistJobs()
        scrape = AsyncMock(side_effect=lambda url: media_result(url))
        try:
            response = manager.submit(
                "owner", PlaylistRequest(urls=POSTS[:2]), tmp_path,
                AsyncMock(), scrape, AsyncMock(),
            )
            job = manager.get(response["jobId"], "owner")
            await job.task
            assert job.status == "READY"
            assert job.public(response["jobId"])["progress"] == 100
            with zipfile.ZipFile(job.directory / "playlist.zip") as bundle:
                assert len(bundle.namelist()) == 2
                assert bundle.namelist()[0].startswith("001-")
                assert bundle.namelist()[1].startswith("002-")
                assert all("/" not in name and "\\" not in name for name in bundle.namelist())
                assert bundle.read(bundle.namelist()[0]) == b"media"
            assert [call.args[0] for call in scrape.await_args_list] == POSTS[:2]

            single = manager.submit("owner", PlaylistRequest(urls=[POSTS[1]]), tmp_path, AsyncMock(), scrape, AsyncMock())
            single_job = manager.get(single["jobId"], "owner")
            await single_job.task
            with zipfile.ZipFile(single_job.directory / "playlist.zip") as bundle:
                assert len(bundle.namelist()) == 1
        finally:
            await manager.close()
    asyncio.run(run())


def test_audio_mode_converts_video_and_ignores_cover(tmp_path):
    async def run():
        create_media(tmp_path)
        manager = PlaylistJobs()
        result = media_result()
        result.data.media.append(result.data.media[0].model_copy(update={"role": "cover", "type": "image"}))

        async def convert(source, output):
            output.write_bytes(b"mp3")
        audio = AsyncMock(side_effect=convert)
        try:
            response = manager.submit("owner", PlaylistRequest(urls=[POSTS[1]], mode="audio"), tmp_path,
                                      AsyncMock(), AsyncMock(return_value=result), audio)
            job = manager.get(response["jobId"], "owner")
            await job.task
            assert job.status == "READY"
            audio.assert_awaited_once()
            with zipfile.ZipFile(job.directory / "playlist.zip") as bundle:
                assert len(bundle.namelist()) == 1
                assert bundle.namelist()[0].endswith(".mp3")
                assert bundle.read(bundle.namelist()[0]) == b"mp3"
        finally:
            await manager.close()
    asyncio.run(run())


def test_failed_items_never_leave_partial_archives(tmp_path):
    async def run():
        create_media(tmp_path)
        manager = PlaylistJobs()
        scrape = AsyncMock(side_effect=[media_result(), HTTPException(400, "Item unavailable")])
        try:
            response = manager.submit("owner", PlaylistRequest(urls=POSTS[:2]), tmp_path, AsyncMock(), scrape, AsyncMock())
            job = manager.get(response["jobId"], "owner")
            await job.task
            assert job.status == "FAILED"
            assert job.error == "Item unavailable"
            assert not job.directory.exists()
        finally:
            await manager.close()
    asyncio.run(run())


def test_size_limit_owner_isolation_rate_limit_and_expiry(tmp_path, monkeypatch):
    async def run():
        create_media(tmp_path)
        manager = PlaylistJobs()
        monkeypatch.setattr(playlists, "MAX_BYTES", 4)
        try:
            response = manager.submit("owner", PlaylistRequest(urls=[POSTS[1]]), tmp_path,
                                      AsyncMock(), AsyncMock(return_value=media_result()), AsyncMock())
            with pytest.raises(HTTPException) as busy:
                manager.submit("owner", PlaylistRequest(urls=[POSTS[1]]), tmp_path, AsyncMock(), AsyncMock(), AsyncMock())
            assert busy.value.status_code == 429
            with pytest.raises(HTTPException) as foreign:
                manager.get(response["jobId"], "other")
            assert foreign.value.status_code == 404
            job = manager.get(response["jobId"], "owner")
            await job.task
            assert job.status == "FAILED"
            assert "limit" in job.error
            assert not job.directory.exists()
            job.expires_at = int(time.time()) - 1
            with pytest.raises(HTTPException):
                manager.get(response["jobId"], "owner")
            assert response["jobId"] not in manager.jobs
        finally:
            await manager.close()
    asyncio.run(run())


@pytest.mark.parametrize("url", [
    "https://evil.example/file.mp4", "/web/media/instagram/post/video.mp4",
    "/media/instagram/../secret", "/media/instagram/post/%2e%2e/secret",
    "/media/instagram/post/%2fetc/passwd",
])
def test_archive_only_reads_local_safe_media(tmp_path, url):
    with pytest.raises(HTTPException):
        cached_asset(tmp_path, url)


def test_archive_refuses_symlinked_files_and_parent_directories(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "video.mp4").write_bytes(b"secret")
    (tmp_path / "post").symlink_to(outside, target_is_directory=True)
    with pytest.raises(HTTPException):
        cached_asset(tmp_path, "/media/instagram/post/video.mp4")
    (tmp_path / "post").unlink()
    (tmp_path / "post").mkdir()
    (tmp_path / "post" / "video.mp4").symlink_to(outside / "video.mp4")
    with pytest.raises(HTTPException):
        cached_asset(tmp_path, "/media/instagram/post/video.mp4")


def test_metadata_only_extractor_canonicalizes_youtube_entries(tmp_path):
    async def run():
        async def execute(*args, **kwargs):
            assert "--skip-download" in args
            assert "--flat-playlist" in args
            kwargs["stdout"].write(json.dumps({
                "title": "My playlist", "entries": [{"id": "abcdefghijk", "url": "abcdefghijk"}],
            }).encode())
            kwargs["stdout"].flush()
            return Mock(returncode=0, wait=AsyncMock())
        with patch("pinchana_server.playlists.asyncio.create_subprocess_exec", side_effect=execute):
            title, urls = await extract_collection("https://music.youtube.com/playlist?list=test", tmp_path)
        assert title == "My playlist"
        assert urls == ["https://music.youtube.com/watch?v=abcdefghijk"]
    asyncio.run(run())


def test_finished_archives_expire_and_shutdown_removes_pending_jobs(tmp_path):
    async def run():
        create_media(tmp_path)
        manager = PlaylistJobs()
        response = manager.submit("owner", PlaylistRequest(urls=[POSTS[1]]), tmp_path,
                                  AsyncMock(), AsyncMock(return_value=media_result()), AsyncMock())
        job = manager.get(response["jobId"], "owner")
        await job.task
        directory = job.directory
        assert directory.exists()
        job.expires_at = int(time.time()) - 1
        manager.prune()
        assert not directory.exists()
        pending = manager.submit("owner", PlaylistRequest(urls=[POSTS[1]]), tmp_path,
                                 AsyncMock(), AsyncMock(return_value=media_result()), AsyncMock())
        pending_job = manager.get(pending["jobId"], "owner")
        await manager.close()
        assert pending_job.task.done()
        assert not pending_job.directory.exists()
        assert manager.jobs == {}
    asyncio.run(run())


def test_native_collection_listing_never_uses_dlp(tmp_path):
    async def run():
        urls = ["https://open.spotify.com/track/one", "https://open.spotify.com/track/two"]
        with patch("pinchana_server.main._playlist_module_payload", AsyncMock(return_value={"title": "Album", "urls": urls})) as listing:
            title, entries = await main._resolve_playlist("https://open.spotify.com/album/abc", tmp_path)
        assert (title, entries) == ("Album", urls)
        listing.assert_awaited_once_with("spotify", "playlist", "https://open.spotify.com/album/abc")
    asyncio.run(run())


def test_mobile_claims_cannot_own_web_playlists():
    with pytest.raises(HTTPException) as failure:
        main._playlist_owner({"typ": "mobile_access", "sub": "installation", "jti": "token"})
    assert failure.value.status_code == 401


def test_web_routes_reject_api_keys_and_foreign_sessions(tmp_path, monkeypatch):
    async def run():
        manager = PlaylistJobs()
        create_media(tmp_path)
        monkeypatch.setenv("TURNSTILE_SESSION_SECRET", "s" * 32)
        monkeypatch.setenv("PINCHANA_API_KEYS", '{"bot":"machine-key"}')
        monkeypatch.setattr(main, "playlist_jobs", manager)
        monkeypatch.setattr(main.storage, "base_path", tmp_path)
        monkeypatch.setattr(main, "_normalized_scrape_response", AsyncMock(return_value=media_result()))
        token, _ = main._issue_web_session()
        other, _ = main._issue_web_session()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                for path in ["/web/playlists/jobs", "/web/playlists/jobs/12345678-1234-1234-1234-123456789abc/file"]:
                    response = await client.request("POST" if path.endswith("/jobs") else "GET", path,
                                                    headers={"X-API-Key": "machine-key"}, json={"urls": [POSTS[1]]})
                    assert response.status_code == 401
                response = await client.post("/web/playlists/jobs", headers=headers, json={"urls": [POSTS[1]]})
                assert response.status_code == 202
                identifier = response.json()["jobId"]
                await manager.get(identifier, main._validate_web_session(token)["nonce"]).task
                response = await client.get(f"/web/playlists/jobs/{identifier}", headers={"Authorization": f"Bearer {other}"})
                assert response.status_code == 404
                response = await client.get(f"/web/playlists/jobs/{identifier}/file", headers=headers)
                assert response.status_code == 200
                assert response.headers["content-type"] == "application/zip"
                assert ".zip" in response.headers["content-disposition"]
                assert response.headers["cache-control"] == "no-store"
                for path in ["/v1/playlists/jobs", "/v1/mobile/playlists/jobs"]:
                    response = await client.post(path, headers=headers, json={"urls": [POSTS[1]]})
                    assert response.status_code == 404
        finally:
            await manager.close()
    asyncio.run(run())
