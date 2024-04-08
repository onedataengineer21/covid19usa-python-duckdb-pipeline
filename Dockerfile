# Use the official Python image as the base image
FROM python:3.12

# Set the working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Execute pipeline
CMD ["python", "./src/etl_script.py"]