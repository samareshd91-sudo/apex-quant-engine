# Prime Samaresh Engine v59.7 ⚡

A highly resilient, distributed Smart Money Concept (SMC) trading engine built for Render.

## Architecture
- **Web Layer:** Streamlit (Health Check & Dashboard)
- **Background Worker:** Jittered interval scanner with memory guardian
- **Ledger:** PostgreSQL-backed idempotent signal delivery with lease-based duplicate protection.
- **Indicators:** Native Pandas & Numpy (No pandas-ta dependency)

## Render Deployment Instructions
1. Create or connect a PostgreSQL database and provide its connection string to the Web Service as `DATABASE_URL`.
2. Ensure the following **Environment Variables** are set in Render:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - `DATABASE_URL` 
3. **Build Command:** `pip install -r requirements.txt`
4. **Start Command:** `streamlit run app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true`

*Note: The engine features an atomic fail-closed design. If the Database is unavailable or atomic claim fails, signal delivery is securely blocked.*

