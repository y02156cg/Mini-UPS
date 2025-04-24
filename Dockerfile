FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y postgresql-client
RUN apt-get update && apt-get install -y iputils-ping netcat-openbsd
COPY ups/requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project
COPY ups/ .

# Expose the port your Django app runs on (if using runserver)
EXPOSE 8000

COPY wait-for-postgres.sh /wait-for-postgres.sh
RUN chmod +x /wait-for-postgres.sh

CMD ["/wait-for-postgres.sh", "python", "manage.py", "runserver", "0.0.0.0:8000"]