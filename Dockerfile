# Dockerfile

FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install dependencies (will now include pinecone and gunicorn)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code and the web entry point
COPY scraper_job.py .
COPY app.py .

# Command to run the production web server (Gunicorn)
# This command binds the Flask app instance named 'app' (from app.py) 
# to the required host/port (0.0.0.0:8080)
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]```

### Fix 3: Ensure `app.py` is Ready for Gunicorn

In your `app.py`, you must comment out the local execution block, as Gunicorn will manage the application startup.

```python
# app.py (Final lines check)
# ... (rest of the Flask code) ...

if __name__ == "__main__":
    # Remove or comment out this block, as Gunicorn handles the startup
    # app.run(host="0.0.0.0", port=PORT) 
    pass # Gunicorn will run the app instead
