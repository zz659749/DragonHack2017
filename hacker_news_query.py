

#Query A How many stories are there?

from google.cloud import bigquery
import webapp2
import uuid
import time

# class
class StoryCount(webapp2.RequestHandler):
    def get(self):
        rs = self.get_result()
        text = 'count %d' % rs[0][0] if rs else 'no record'
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
        sql = """select count(stories.id) as stories_Count
                  from `bigquery-public-data.hacker_news.stories` as stories

               """

       
        client = bigquery.Client('extreme-core-158121')
        #query_results = client.run_sync_query(sql)
        job = client.run_async_query(str(uuid.uuid4()), sql)
        ds = client.dataset('results')
        if not ds.exists():
            ds.create()
            ds.reload()

        tbl = ds.table('table_a')
        #tbl.reload()
        job.destination = tbl 

        # Use standard SQL syntax for queries.
    	# See: https://cloud.google.com/bigquery/sql-reference/
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
