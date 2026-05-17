# The Tape outbox dispatcher image — your connector module(s) + tape-py,
# entrypoint = the outbox loop. The container's job is one of:
#
#   * own the (PENDING + OUTBOX) → CONFIRMED transition for one or more
#     non-idempotent upstreams, via registered connectors.
#
# Build context should include your connector module(s) and requirements;
# this is a template.
FROM python:3.12-slim
WORKDIR /app

# 1. install Tape and (optionally) the Pub/Sub extra if any connector uses it:
#       pip install 'tape-py[gcp-pubsub]'
# 2. install whatever HTTP client / domain libraries your connectors need
#    (httpx, requests, google-cloud-pubsub, etc.).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
# The CMD is overridden by the Service args (which carry --load module:attr
# entries and the TAPE_URL). This default mirrors the manifest for local
# testing.
CMD ["tape-outbox", "--url", "tape://tape-server:7878", "--interval", "1"]
