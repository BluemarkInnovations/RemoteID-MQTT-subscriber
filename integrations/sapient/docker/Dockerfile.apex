# Dstl Apex SAPIENT middleware (headless server only), pinned to a commit of
# https://github.com/dstl/Apex-SAPIENT-Middleware (Apache 2.0 / Crown copyright, see its licence files).
# Used as the conformance check for sapient_gateway.py: it validates every message against
# BSI Flex 335 v2.0 and only passes valid ones on to the DMM ("Peer") connection.
FROM python:3.11-slim

ARG APEX_COMMIT=0c8591ae97f2dc336eff299845880d5c73ee75b7
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && git clone https://github.com/dstl/Apex-SAPIENT-Middleware.git /apex \
    && git -C /apex checkout "$APEX_COMMIT" \
    && pip install --no-cache-dir -r /apex/requirements.txt \
    && pip install --no-cache-dir --no-deps /apex

WORKDIR /apex
RUN mkdir -p /apex/data && useradd -r -m apex && chown -R apex /apex/data
USER apex
ENV PYTHONPATH=/apex PYTHONUNBUFFERED=1
CMD ["apex", "/config/apex_config.json"]
