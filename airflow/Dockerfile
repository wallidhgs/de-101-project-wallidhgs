FROM apache/airflow:2.5.3

# Set the working directory in the container
WORKDIR /app

# Install apt-get packages as root
USER root

# apt-get update
# RUN apt-get update \
#   && apt-get install -y --no-install-recommends \
#         unzip \
#   && apt-get autoremove -yqq --purge \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Change to airflow user, to avoid using pip as root
USER airflow

# Install the necessary dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# awscli is installed in a separate command as is part of DAG overall requirements and it's not referenced in code
# RUN pip install --no-cache-dir --user awscli

# Change password if you are debugging
# RUN echo 'root:Docker!' | chpasswd
