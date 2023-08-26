import asyncio
import json
import os
import logging

from aiohttp import web
from aiohttp_basicauth import BasicAuthMiddleware

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def meltano(*args, env = {}, **kwargs):
    return await asyncio.create_subprocess_exec(
        "meltano",
        *args,
        cwd=os.getenv("MELTANO_PROJECT_ROOT", os.getcwd()),
        env={
            **os.environ,
            "NO_COLOR": "1",
            **env
        },
        **kwargs
    )

async def run_scheduler(_app):
    logger.info(f"Starting scheduler")
    process = await meltano("invoke", "airflow", "scheduler")

    returncode = await process.wait()
    logger.info(f"Scheduler exited with code {returncode}")

    if returncode != 0:
        raise Exception(f"Scheduler exited with code {returncode}")

    yield

    if process.returncode is None:
        logger.info("Stopping scheduler")
        process.terminate()
        await process

async def run_app(name, input="", env={}):
    # TODO: Logs. Call via Airflow? (Not all apps may have a schedule/DAG) May help with retries
    logger.info(f"Running app '{name}' with input: {input}")

    env["TAP_PIXLET_APP_INPUT"] = input

    # TODO: meltano invoke airflow dags trigger {name} --conf '{"input": "{input}"}'
    # TODO: Means this is not testable without Airflow... May also not work without scheduler running?
    # TODO: Depend on RUN_SCHEDULER?
    process = await meltano(
        "run",
        name,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )
    stdout, _ = await process.communicate()
    output = stdout.decode('utf-8').splitlines()

    if process.returncode == 0:
        logger.info(f"App '{name}' ran successfully")
        return (True, output)
    else:
        logger.info(f"App '{name}' failed with error: {stdout}")
        return (False, output)

async def handle_update(request, env = {}):
    app_name = request.match_info["app_name"]
    input = await request.text()

    success, output = await run_app(app_name, input, env)

    if success:
        status = 200
        result = {"output": output}
    else:
        status = 500
        result = {"error": output}

    return web.Response(
        status=status,
        headers={'Content-type': 'application/json'},
        text=json.dumps(result)
    )

async def handle_notify(request):
    return await handle_update(
        request,
        {
            "TAP_PIXLET_INSTALLATION_ID": "notification",
            "TAP_PIXLET_BACKGROUND": "false"
        }
    )

def main():
    port = int(os.getenv("SERVER_PORT") or 1234)
    username = os.getenv("SERVER_USERNAME")
    password = os.getenv("SERVER_PASSWORD")
    should_run_scheduler = os.getenv("SERVER_RUN_SCHEDULER", "true") == "true"

    middlewares = []
    if username and password:
        # TODO: Fail if not provided? Auth required? Only run Airflow scheduler in that case
        middlewares.append(BasicAuthMiddleware(username=username, password=password))

    app = web.Application(middlewares=middlewares)

    if should_run_scheduler:
        app.cleanup_ctx.append(run_scheduler)

    app.add_routes([
        web.post('/apps/{app_name}/update', handle_update),
        web.post('/apps/{app_name}/notify', handle_notify),
        # TODO: web.post('/apps/{app_name}/preview', handle_preview), # Using target-webp with output_path
        # TODO: web.post('/apps/{app_name}/pause', handle_pause), # Using Airflow CLI
        # TODO: web.post('/apps/{app_name}/unpause', handle_unpause), # Using Airflow CLI
        # TODO: web.post('/apps/{app_name}/delete', handle_unpause), # Based on installation ID
    ])

    web.run_app(app, port=port)
