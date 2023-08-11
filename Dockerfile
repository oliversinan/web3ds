FROM python:3.9

# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install cryo_cli
# Set up code directory
WORKDIR /app

# Install Linux dependencies
RUN apt-get update && apt-get install -y libssl-dev

COPY . /app
RUN pip install -r requirements.txt
CMD ["python", "-u", "src/main.py"]