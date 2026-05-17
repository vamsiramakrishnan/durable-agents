# {{ name }} — agent + reactor image (one image, two entrypoints).
#
# Cloud Run service for the agent: `docker run ... python -m app.main`
# Cloud Run service for the reactor: `python -m tape.reactors --runner-from app.agent:build_runner`
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY app ./app
RUN pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1
CMD ["python", "-c", "from app.agent import build_app; print(build_app().name)"]
