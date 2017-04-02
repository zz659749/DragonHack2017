

#Query B Which story has received the lowest score?

from google.cloud import bigquery
import webapp2
import uuid
import time

#class
class Score(webapp2.RequestHandler):
    def get(self):
        rs = self.get_result()
        text = """
        <table>
        <tr><td>id</td><td>title</td><td>score</td><td>author</td></tr>
        """
        for i in xrange(len(rs)):
            text += '<tr>'
            text += '<td>%d</td>' % rs[i][0]
            text += '<td>%s</td>' % rs[i][1]
            text += '<td>%s</td>' % rs[i][2]
            text += '<td>%s</td>' % rs[i][3]
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
                SELECT id, title, score, author
                FROM `bigquery-public-data.hacker_news.stories` AS stories
                WHERE score is not null
                AND
                score <= (SELECT MIN(score)
                FROM `bigquery-public-data.hacker_news.stories`)
                LIMIT 5

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

        tbl = ds.table('table_b')
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
