FROM python:3.11-slim

# ffmpeg = কমপ্রেশন; gcc = tgcrypto বিল্ড ফলব্যাক (ডাউনলোড/আপলোড স্পিডের জন্য জরুরি)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 user
USER user
ENV PATH="/home/user/.local/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    WORKDIR=/tmp/work

WORKDIR /home/user/app
COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=user . .

CMD ["python", "bot.py"]
