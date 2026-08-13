# TrueVibe 🌐

TrueVibe is a modern web-based social network built for seamless real-time interaction, digital expression, and community engagement.

---

## 🚀 Features

- **Real-time Interaction:** Fast and interactive web frontend.
- **Backend API:** Powered by Node.js and Express for high performance.
- **Containerized Deployment:** Dockerized setup for effortless hosting and scaling.
- **Clean Architecture:** Simple structure optimized for speed and readability.

---

## 🛠️ Project Structure

```text
truevibe/
├── public/           # Static frontend files (HTML, CSS, JS)
├── Dockerfile        # Container build instructions
├── package.json      # Dependencies and scripts
├── server.js         # Entry point for the backend server
└── README.md         # Project documentation
```

---

## 💻 Getting Started

### Prerequisites

Ensure you have the following installed on your machine:

- Node.js (v16+ recommended)
- npm
- Docker (optional, for containerized run)

### 🔧 Local Installation

1. Clone the repository:

```bash
git clone https://github.com/qmay-eu/truevibe.git
cd truevibe
```

2. Install dependencies:

```bash
npm install
```

3. Start the development server:

```bash
npm start
```

(or `node server.js`)

4. Open your browser and navigate to:

```
http://localhost:3000
```

---

## 🐳 Running with Docker

If you prefer using Docker to run the application in a container:

1. Build the Docker image:

```bash
docker build -t truevibe .
```

2. Run the container:

```bash
docker run -p 3000:3000 truevibe
```

3. Access the app at [http://localhost:3000](http://localhost:3000).