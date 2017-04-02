

#Query D List how many stories where posted by each author on nytimes.com and wired.com.


from google.cloud import bigquery
import webapp2
import uuid
import time

#class
class storyList(webapp2.RequestHandler):
    def get(self):
        rs = self.get_result()
        text = """
        <table>
        <tr><td>domain</td><td>stories_author</td><td>stories_count</td></tr>
        """
        for i in xrange(len(rs)):
            text += '<tr>'
            text += '<td>%s</td>' % rs[i][0]
            text += '<td>%s</td>' % rs[i][1]
            text += '<td>%d</td>' % rs[i][2]
            text += '</tr>'
        text += '</table>'
        self.response.write(text)
    #async waiting
    def __wait_for_job(self, job):
        while True:
            job.reload()  # Refreshes the state via a GET request.
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.errors)
                return
            time.sleep(1) 

    def get_result(self):
        sql = """
                SELECT
                REGEXP_EXTRACT(stories.url, r'http.*\://.*\.([a-zA-Z_-]+\.com)') AS domains, stories.author,
                COUNT(stories.id) AS stories_count 
                FROM `bigquery-public-data.hacker_news.stories` AS stories
                where stories.url is not null
                and (REGEXP_CONTAINS(stories.url, r'\.nytimes\.com') or
                REGEXP_CONTAINS(stories.url, r'\.wired\.com'))
                GROUP BY domains, stories.author 
                ORDER BY stories.author

              """

        client = bigquery.Client('extreme-core-158121')
        #query_results = client.run_sync_query(sql)

        # Use standard SQL syntax for queries.
      # See: https://cloud.google.com/bigquery/sql-reference/
        job = client.run_async_query(str(uuid.uuid4()), sql)
        ds = client.dataset('results')
        if not ds.exists():
            ds.create()
            ds.reload()

        tbl = ds.table('table_d')
        #tbl.reload()
        job.destination = tbl 
        job.use_legacy_sql = False

        job.begin()
        self.__wait_for_job(job)


        # Drain the query results by requesting a page at a time.
        query_results = job.results()
        page_token = None
        rs = []
        while True:
            rows, total_rows, page_token = query_results.fetch_data(
                max_results=500,
                page_token=page_token)

            rs += rows

            if not page_token:
                break
        return rs
