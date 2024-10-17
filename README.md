# rtg-automotive-frontend


TO DO:

- Add a feature that lets you see whether there is data for each supplier and for each date, show some kind of dataframe that shows you all the data available
- Log more events
- Build dead letter queue for lambda functions
- Create email notifications when there are certain number of errors in the dead letter queue, thresholds can be set for each lambda function

# Commands:

- Re-pull updated package changes:

rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt