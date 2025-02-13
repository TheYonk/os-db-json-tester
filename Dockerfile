# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirement.txt .

# Install okg-config
RUN apt update && apt install -y \ 
	pkg-config \
	default-libmysqlclient-dev \
	libpq-dev \
	build-essential \
	jq \
	gettext \
	&& rm -rf /var/lib/apt/lists/*

# Install the dependencies
RUN pip install --no-cache-dir -r requirement.txt

# Copy the rest of the application code into the container
COPY . .

# Set the environment variable for Python
ENV PYTHONUNBUFFERED=1

# Specify the command to run the application
CMD ["./start_bench.sh"]

