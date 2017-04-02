

#python main to work with the apps and database
import webapp2

class MainPage(webapp2.RequestHandler):
    def get(self):
        
        self.response.out.write("""
          <html>
            <body>
              <form action="/scoreAvg" method="post">
                <div><p>Which url has the best story in 2010?</p></div>
                <div><a href="http://localhost:8080/scoreAvg">Top 4 of Hacker News</a></div>
                <div><a href="https://datastudio.google.com/open/0B0J2JkD2fP9tblhZSzdFSG5NY2M">Bar chart</a></div>
              </form>
            </body>
          </html>

          <html>
            <body>
              <form action="/storyCount" method="post">
                <div><p>How many stories are there?</p></div>
                <div><a href="http://localhost:8080/storyCount">Q1</a></div>
              </form>
            </body>
          </html>

          <html>
            <body>
              <form action="/score" method="post">
                <div><p>Which story has the lowest score?</p></div>
                <div><a href="http://localhost:8080/score">Q2</a></div>
              </form>
            </body>
          </html>
          
          <html>
            <body>
              <form action="/storyList" method="post">
                <div><p>List number of stories of each author in nytimes.com and wired.com?</p></div>
                <div><a href="http://localhost:8080/storyList">Q4</a></div>
                <div><a href="https://datastudio.google.com/open/0B0J2JkD2fP9tU0U3WjRVSlV3czg">Bar Chart</a></div>
             </form>
            </body>
          </html>



          """)

#call the classes
app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/storyCount', 'hacker_news_query.StoryCount'),
    ('/score', 'scoreLowest.Score'),
    ('/scoreAvg','url_avg.scoreAvg'),
    ('/storyList', 'story_list.storyList')
], debug=True)
