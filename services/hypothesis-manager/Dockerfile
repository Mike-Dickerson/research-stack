FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Fix line endings and make entrypoint executable (Windows compatibility)
RUN sed -i 's/\r$//' entrypoint.sh && chmod +x entrypoint.sh

# Use entrypoint to fix permissions before starting
ENTRYPOINT ["/app/entrypoint.sh"]
