# 1. Base image
FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1

# 2. Set the working directory to the root of our app
WORKDIR /app

# 3. Install uv inside the container
RUN pip install uv

# 4. Copy the entire project into the /app folder inside the container
COPY . /app

# 5. Use uv to install all your dependencies 
# (This automatically creates a .venv folder inside /app)
RUN uv sync

# 6. The Magic Tricks!
# Tell the container to always use the Python inside the uv virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Tell Python that /app is the root folder for all your internal imports
ENV PYTHONPATH="/app"