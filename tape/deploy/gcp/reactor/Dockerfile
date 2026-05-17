# The Tape reactor image — your agent package + tape-py, entrypoint = the reactor loop.
# Build context should be wherever your agent package and these files live; this is a
# template. (If you instead run the agent on Cloud Run, you don't need a separate reactor
# image at all — add a second container to the agent service with entrypoint
#   tape-reactors --runner-from my_agent.app:build_runner --url tape://...
# .)
FROM python:3.12-slim
WORKDIR /app

# 1. install the Tape SDK (from your private index, a wheel, or the repo)
#    pip install tape-py
# 2. install your agent package — it must register @tape.effect(status_check=…) on import
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt   # should include: tape-py, google-adk, google-cloud-aiplatform, grpcio, google-auth, <your agent pkg>
COPY . .

# Cloud Run sets $PORT but this loop doesn't serve HTTP; if you want a health endpoint,
# wrap main() behind a tiny HTTP server. For a pure worker, a min-instances=1 Cloud Run
# service runs main() forever (Cloud Run keeps the instance because it's "always allocated").
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "main"]
