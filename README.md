# PimioPilot
Semi-automated stock pilot with AI forecasts

## License
This project is licensed under the AGPL-3.0 License.
See the [LICENSE](LICENSE) file for the full license text.

### Third-Party Software
This project uses:

- **yfinance** (https://github.com/ranaroussi/yfinance)
  Licensed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
  Copyright © Ran Aroussi and contributors.




## Docker Quick Start

The following steps will help you quickly run this project inside a Docker environment:

### 1. Build the image
In the project root directory (where the `Dockerfile` is located), run:
```bash
docker build -t pimio-pilot .
```

### 2. Run the script
If the `CMD` in the `Dockerfile` is set to `["python"]`, run instead:
```bash
docker run --rm -it pimio-pilot python fetch_taiwan_stock_yfinance.py
```

### 3. Development mode (live code updates)
Mount the current directory into the container to avoid rebuilding the image every time you change the code:
```
bash
docker run --rm -it -v "$(pwd)":/app pimio-pilot python fetch_taiwan_stock_yfinance.py
```

### 4. Run with Docker Compose (optional)

You can also use **Docker Compose** to simplify running the project.

Then build and run with:

```bash
docker compose up --build
```

Stop the container with:

```bash
docker compose down
```

With these steps, you can use yfinance in a Docker environment to fetch Taiwan stock data.


## Usage

### Run Locally (without Docker)
```bash
pip install -r requirements.txt
python fetch_taiwan_stock_yfinance.py
```

### Run with Docker
See the [Docker Quick Start](#docker-quick-start) section for details on building and running inside Docker.
