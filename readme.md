📋 Setup Instructions
1. Create Repository Structure
Create a new GitHub repository and add these files:
your-repo/
├── .github/
│   └── workflows/
│       └── fetch-weather.yml
├── data/
│   └── .gitkeep
├── Dockerfile
├── fetch_data.py
├── generate_index.py
└── README.md
2. Configure GitHub Secrets
Add your TTN API token as a repository secret:

Go to your GitHub repository
Click Settings → Secrets and variables → Actions
Click New repository secret
Name: TTN_TOKEN
Value: Your TTN API token (paste the full token)
Click Add secret

3. Enable GitHub Pages

Go to Settings → Pages
Under Source, select Deploy from a branch
Select branch: gh-pages
Select folder: / (root)
Click Save

4. Initialize the Data Directory
Create an empty .gitkeep file in the data/ directory:
bashmkdir -p data
touch data/.gitkeep
git add data/.gitkeep
git commit -m "Initialize data directory"
git push
5. Run the Workflow
Manual Trigger (First Run)

Go to Actions tab
Click on Fetch Weather Data workflow
Click Run workflow → Run workflow

Automatic Schedule
The workflow runs automatically every Sunday at 00:00 UTC.
To change the schedule, edit the cron expression in .github/workflows/fetch-weather.yml:
yamlschedule:
  - cron: '0 0 * * 0'  # Every Sunday at midnight UTC
6. Access Your Data
After the first successful run, your data will be available at:
https://YOUR-USERNAME.github.io/YOUR-REPO-NAME/
📊 Data Output

Weekly files: weather_data_YYYY_WWW.csv (e.g., weather_data_2026_W01.csv)
Latest file: latest.csv (always contains the most recent week's data)
Resampling: Data is resampled to 30-minute intervals
Interpolation: Linear interpolation for meteorological variables, forward-fill for battery/status

🐳 Docker (Optional)
To run locally with Docker:
bashexport TTN_TOKEN="your-token-here"
docker build -t weather-fetcher .
docker run -e TTN_TOKEN=$TTN_TOKEN -v $(pwd)/data:/app/data weather-fetcher
🔧 Customization
Change Data Collection Period
Edit fetch_data.py line 11:
pythonparams = {"last": "168h"}  # 168h = 7 days (1 week)
Change Resampling Interval
Edit fetch_data.py line 70:
python.apply(lambda g: g.resample("30min").first())  # Change "30min" to desired interval
Modify Schedule
Edit .github/workflows/fetch-weather.yml:

Daily: cron: '0 0 * * *'
Twice weekly: cron: '0 0 * * 0,3' (Sunday and Wednesday)
Monthly: cron: '0 0 1 * *'

🐛 Troubleshooting
Workflow Fails

Check that TTN_TOKEN secret is set correctly
Verify your token has read access to the TTN application
Check the Actions logs for detailed error messages

No Data Collected

Verify your devices are sending data to TTN
Check that the application ID in fetch_data.py matches your TTN application

GitHub Pages Not Working

Ensure the gh-pages branch exists after first workflow run
Check Pages settings are configured correctly
Wait a few minutes for Pages to deploy after workflow completes
