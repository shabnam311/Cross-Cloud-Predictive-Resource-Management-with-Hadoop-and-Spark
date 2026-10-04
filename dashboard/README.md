# Cross-Cloud Resource Forecasting Dashboard

Static reporting front end for **Cross-Cloud Predictive Resource Management with Hadoop and Spark**.

---

## 1. Quick Start

### Install Dependencies
```bash
cd dashboard
npm install
```

### Regenerate Mock Data
```bash
npm run mock
```

### Run Local Development Server
```bash
npm run dev
```
Open `http://localhost:5173` in your browser.

### Build for Production
```bash
npm run build
```
The compiled static assets are output to `dashboard/dist`.

### Preview Production Build
```bash
npm run preview
```

---

## 2. Replacing Mock Data with Real Spark Pipeline Output

The dashboard is completely decoupled from the data pipeline and renders plain static JSON files stored in `dashboard/public/data/`.

1. Run the Spark export script inside your Hadoop/Spark cluster:
   ```bash
   docker exec -it spark-master-cloud /opt/spark/bin/spark-shell -i /opt/spark/work-dir/exportDashboardData.scala
   ```
2. Copy the 6 exported JSON files from HDFS/local export into `dashboard/public/data/`:
   - `overview.json`
   - `forecasts.json`
   - `comparison.json`
   - `recommendations.json`
   - `models.json`
   - `pipeline.json`
3. Ensure the top-level `isSample` property in each JSON file is set to `false`. The UI and footer will automatically switch from **Sample data** to **Pipeline output**.

---

## 3. Deployment

### Deploying to Vercel
1. Import the GitHub repository into your Vercel dashboard.
2. In the project settings, set the **Root Directory** to `dashboard`.
3. Keep the default build settings:
   - **Framework Preset**: Vite
   - **Build Command**: `npm run build`
   - **Output Directory**: `dist`
4. Click **Deploy**. No environment secrets or serverless functions are required.

### Deploying to GitHub Pages
1. In your GitHub repository, navigate to **Settings** > **Pages**.
2. Under **Build and deployment** > **Source**, select **GitHub Actions**.
3. Push changes to the `main` branch. The automated workflow in `.github/workflows/deploy-pages.yml` will automatically build the dashboard with the correct repository base path and publish it to GitHub Pages.
