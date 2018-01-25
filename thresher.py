import csv
import requests
import optparse
from bs4 import BeautifulSoup
from pymongo import MongoClient

# Set up mongoDB as our persistent datastore
client = MongoClient('mongodb://127.0.0.1:27017')
db = client.test

tech_headlines = []
cursor = db.tech_headlines.find()

if cursor:
  for th in cursor:
    tech_headlines.append( th )


def do_csv(option, opt_str, value, parser):

   client = MongoClient('mongodb://127.0.0.1:27017')
   db = client.test

   tech_headlines = []
   cursor = db.tech_headlines.find()

   if cursor:
      for th in cursor:
        tech_headlines.append( th )

   print "in do_csv"   

   parser.values.csv_trigger = True

   with open('tech_headlines.csv', 'w') as outfile:

     fields = ['_id', 'url', 'date', 'title' ]
     write = csv.DictWriter(outfile, fieldnames=fields)
     write.writeheader()

     for th in tech_headlines:

       row = dict(th)
       unidict = {k.encode('utf8'): v.encode('utf8') for k, v in row.items()}

       write.writerow(unidict)

def main():

    p = optparse.OptionParser(description=' Get 100 tech headline articles with option to output datastore to CSV')
    p.add_option('--csv', '-C', default="csvflag", help="Flag to trigger CSV creation over data retreive", action="callback", callback=do_csv)

    options, arguments = p.parse_args()

    if hasattr(options, 'csv_trigger'):
      print "Made CSV File ..."
    else:
      print "Getting some Data ..." 


      # Get HTML from Technology url - enable cookies

      url_suffix = ""
      tech_url = 'http://www.yangtse.com/app/internet/'
      user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'

      session = requests.Session()

      # Get 9 pages worth of stories to ensure 100 tech headlines
      story_count = len(tech_headlines)
      for i in range(11):

        if i > 1:
            url_suffix = "index_" + str(i) + ".html"

        res = session.get( tech_url + url_suffix, headers={'User-Agent': user_agent})

        # Run our HTML response through beautiful soup for sanity
        soup = BeautifulSoup(res.text, "html.parser")

        boxes = soup.findAll("div", {"class": "box"})

        for b in boxes:

            title_box = b.find("div", {"class": "box-text-title"})
            url = title_box.find("a")["href"]
            title = title_box.text

            time_box = b.find("div", {"class": "box-text-time"})
            time = time_box.find("span").text

            # Create custom _id to skirt bulk import error
            ident = url.strip(tech_url) 
            ident = ident.strip('.html')

            headline = {
                "_id": ident,
                "url": url,
                "date": time,
                "title": title
            }

            # Only add if it's a distinct URL and we're not over 100
            if not any(th['url'] == headline['url'] for th in tech_headlines) and story_count < 100:
                story_count = story_count + 1
                tech_headlines.append( headline )


      db.tech_headlines.insert_many( tech_headlines )

if __name__ == '__main__':
  main()
