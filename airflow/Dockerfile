FROM apache/airflow:2.5.3

# Set the working directory in the container
WORKDIR /app

# Install apt-get packages as root
USER root
# Install Java

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         default-jdk 

# Copy the requirements file into the container
COPY requirements.txt .

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
  && unzip awscliv2.zip \
  && sudo ./aws/install

# Change to airflow user, to avoid using pip as root
USER airflow

# Install the necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt
