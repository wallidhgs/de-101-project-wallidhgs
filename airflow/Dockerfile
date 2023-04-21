FROM apache/airflow:2.5.3

# Set the working directory in the container
WORKDIR /app

# Install apt-get packages as root
USER root

# apt-get update
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
        unzip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
  && unzip awscliv2.zip \
  && sudo ./aws/install

# Copy the requirements file into the container
COPY requirements.txt .

# Change to airflow user, to avoid using pip as root
USER airflow

# Install the necessary dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Change password if you are debugging
# RUN echo 'root:Docker!' | chpasswd
