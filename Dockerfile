FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install dependencies
COPY ups/requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project
COPY ups/ .

# Expose the port your Django app runs on (if using runserver)
EXPOSE 8000

CMD ["python", "web.py"]
