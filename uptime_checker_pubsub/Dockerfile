# Use a slim Python 3.12 image as the base
FROM python:3.12-slim

# Set the working directory
WORKDIR /home/udaytkumar_consult/uptime_checker_pubsub

# FIX: Install the necessary runtime and development packages for SQLite
# libsqlite3-0 provides the libsqlite3.so.0 file.
RUN apt-get update && apt-get install -y libsqlite3-0 libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Set the command to run the function (Replace 'uptime_handler' if needed)
CMD ["functions-framework", "--target", "uptime_handler", "--port", "8080"]
