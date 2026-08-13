# TrueVibe 🌐

TrueVibe is a modern web-based social network built for seamless real-time interaction, digital expression, and community engagement.

---

## 🚀 Features

- **Real-time Interaction** – Fast interactive web frontend with WebSocket chat
- **Backend API** – Powered by Node.js + Express
- **PostgreSQL Database** – Full relational data storage
- **TikTok-grade Video Feed** – Personalized recommendation engine with collaborative filtering
- **Adaptive HLS Streaming** – 720p / 480p / 240p with 2s segments (fluent-ffmpeg)
- **Smart Ad Recommendation** – Contextual ads with user interest profiles
- **Admin Dashboard** – Advanced user tracking, analytics, ban system, reports
- **Containerized Deployment** – Docker support (optional)

---

## 🛠️ Project Structure

```text
truevibe/
├── public/                 # Static frontend files (HTML, CSS, JS, uploads)
├── Dockerfile              # Container build instructions
├── package.json            # Dependencies and scripts
├── server.js               # Main backend entry point
└── README.md               # Project documentation
```

---

## 💻 Getting Started

### Prerequisites

Make sure you have the following installed:

| Software       | Version / Notes                    |
|----------------|------------------------------------|
| **Node.js**    | v16+ recommended                   |
| **npm**        | Comes with Node.js                 |
| **PostgreSQL** | **Required** – v13+ recommended    |
| **Docker**     | Optional (for containerized run)   |
| **ffmpeg**     | Recommended (video compression + HLS) |

---

## 🐘 PostgreSQL Setup (Required)

TrueVibe uses PostgreSQL as its only database. You **must** install and start PostgreSQL before running the server.

### 1. Install PostgreSQL

**Ubuntu / Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

**macOS (Homebrew):**
```bash
brew install postgresql@16
brew services start postgresql@16
```

**Windows:**  
Download and install from [https://www.postgresql.org/download/windows/](https://www.postgresql.org/download/windows/)

### 2. Start PostgreSQL

```bash
# Linux
sudo systemctl start postgresql
sudo systemctl enable postgresql

# macOS
brew services start postgresql@16
```

### 3. Create database and user

Connect to PostgreSQL as the default superuser:

```bash
sudo -u postgres psql
```

Then run these commands:

```sql
CREATE USER yeah_user WITH PASSWORD 'yeah_super_secret';
CREATE DATABASE yeah_db OWNER yeah_user;
GRANT ALL PRIVILEGES ON DATABASE yeah_db TO yeah_user;
\q
```

> The server connects with these exact credentials (hardcoded in `server.js`):
>
> | Setting  | Value              |
> |----------|--------------------|
> | Host     | `localhost`        |
> | Port     | `5432`             |
> | Database | `yeah_db`          |
> | User     | `yeah_user`        |
> | Password | `yeah_super_secret`|

### 4. (Optional) Allow password authentication

If you get authentication errors, edit `pg_hba.conf` and change the local method to `md5` or `scram-sha-256`, then restart PostgreSQL.

---

## 🔧 Local Installation

1. **Clone the repository**

```bash
git clone https://github.com/qmay-eu/truevibe.git
cd truevibe
```

2. **Install dependencies**

```bash
npm install
```

Recommended packages (if not already in `package.json`):

```bash
npm install express multer express-session compression pg dotenv ws fluent-ffmpeg
npm install @ffmpeg-installer/ffmpeg @ffprobe-installer/ffprobe   # optional but recommended
```

3. **(Optional) Configure environment variables**

Create a `.env` file in the project root:

```env
OPENROUTER_API_KEY=your_openrouter_api_key_here
AI_MODEL=meta-llama/llama-4-maverick
NODE_ENV=development
```

If `OPENROUTER_API_KEY` is missing, the AI assistant runs in demo mode.

4. **Start the server**

```bash
npm start
```

or

```bash
node server.js
```

5. **Open the app**

```
http://localhost:3001
```

> **Note:** The server runs on port **3001** (not 3000).

---

## 🐳 Running with Docker

1. **Build the image**

```bash
docker build -t truevibe .
```

2. **Run the container**

```bash
docker run -p 3001:3001 truevibe
```

3. Access the app at [http://localhost:3001](http://localhost:3001)

> Make sure PostgreSQL is reachable from the container (use Docker networking or host network if needed).

---

## 📦 Main Technologies

- **Backend:** Node.js, Express
- **Database:** PostgreSQL (`pg`)
- **Realtime Chat:** WebSocket (`ws`)
- **Video Processing:** fluent-ffmpeg + HLS adaptive streaming
- **AI Assistant:** OpenRouter API
- **Sessions:** express-session
- **Uploads:** multer
- **Compression:** compression middleware

---

## 🔐 Important Notes

- Admin credentials are defined in `server.js` (change them in production).
- The database credentials are currently hardcoded – consider moving them to environment variables for production.
- Make sure the folders `public/uploads/`, `public/uploads/videos/`, `public/uploads/ads/` and `public/uploads/avatars/` exist and are writable.
- For adaptive HLS streaming, `ffmpeg` must be available in the system PATH (or install the optional `@ffmpeg-installer` packages).
