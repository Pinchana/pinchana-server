import asyncio
import json
import uuid
from unittest.mock import patch

import httpx

from pinchana_server import main

JOB = str(uuid.uuid4())
ENV = {"PINCHANA_API_KEYS": '{"bot":"bot-secret","other":"other-secret"}',
       "DLP_ENABLED": "true", "DLP_URL": "http://dlp.internal",
       "DLP_GATEWAY_TOKEN": "g" * 32, "DLP_OWNER_SECRET": "o" * 32}


class VideoStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"video"


def test_machine_jobs_preserve_authentication_ownership_and_stream_files():
    async def run():
        owners = []
        async def upstream(request):
            assert request.headers["x-dlp-service-token"] == "g" * 32
            assert "x-api-key" not in request.headers
            owners.append(request.headers["x-job-owner"])
            if request.url.path.endswith("/file"):
                return httpx.Response(200, stream=VideoStream(), headers={
                    "Content-Disposition": 'attachment; filename="video.mp4"', "Content-Type": "video/mp4"})
            if request.url.path.endswith("/submit"):
                assert json.loads(request.content)["quality"] == "720p"
                return httpx.Response(200, json={"jobId": JOB, "status": "QUEUED"})
            return httpx.Response(200, json={"jobId": JOB, "status": "READY"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as forward:
            with patch.dict("os.environ", ENV), patch.object(main, "forward_client", forward):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://gateway") as client:
                    headers = {"X-API-Key": "bot-secret", "X-Pinchana-User": "123"}
                    allocated = await client.post("/v1/dlp/jobs", headers=headers)
                    assert allocated.status_code == 200
                    submitted = await client.post(f"/v1/dlp/jobs/{JOB}/submit", headers=headers,
                                                  json={"url": "https://youtube.com/watch?v=abcdefghijk", "quality": "720p"})
                    assert submitted.status_code == 200
                    assert (await client.get(f"/v1/dlp/jobs/{JOB}", headers=headers)).status_code == 200
                    response = await client.get(f"/v1/dlp/jobs/{JOB}/file", headers=headers)
                    assert response.content == b"video"
                    assert response.headers["content-disposition"] == 'attachment; filename="video.mp4"'
                    assert response.headers["cache-control"] == "no-store"
                    assert len(set(owners)) == 1
                    await client.get(f"/v1/dlp/jobs/{JOB}", headers={**headers, "X-Pinchana-User": "456"})
                    assert owners[-1] != owners[0]
                    await client.get(f"/v1/dlp/jobs/{JOB}", headers={**headers, "X-API-Key": "other-secret"})
                    assert owners[-1] not in {owners[0], owners[-2]}
    asyncio.run(run())


def test_machine_routes_require_key_and_user_and_honor_feature_flag():
    async def run():
        with patch.dict("os.environ", {**ENV, "DLP_ENABLED": "false"}):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://gateway") as client:
                for path in ("/v1/dlp/capabilities", f"/v1/dlp/jobs/{JOB}", f"/v1/dlp/jobs/{JOB}/file"):
                    assert (await client.get(path)).status_code in {401, 422}
                    assert (await client.get(path, headers={"X-API-Key": "bot-secret"})).status_code == 422
                headers = {"X-API-Key": "bot-secret", "X-Pinchana-User": "123"}
                assert (await client.post("/v1/dlp/jobs", headers=headers)).status_code == 503
                capabilities = await client.get("/v1/dlp/capabilities", headers=headers)
                assert capabilities.json() == {"available": False, "capabilities": None}
                assert (await client.post("/v1/dlp/jobs", headers={**headers, "X-Pinchana-User": "../123"})).status_code == 422
    asyncio.run(run())


def test_machine_job_cannot_read_another_users_result():
    async def run():
        with patch.dict("os.environ", ENV):
            first_owner = main._dlp_job_owner(main._machine_dlp_owner("bot", "123"))
        def upstream(request):
            status = 200 if request.headers["x-job-owner"] == first_owner else 404
            return httpx.Response(status, json={"jobId": JOB} if status == 200 else {"detail": "Job not found"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as forward:
            with patch.dict("os.environ", ENV), patch.object(main, "forward_client", forward):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://gateway") as client:
                    response = await client.get(f"/v1/dlp/jobs/{JOB}", headers={"X-API-Key": "bot-secret", "X-Pinchana-User": "456"})
                    assert response.status_code == 404
    asyncio.run(run())
