import queue
import pandas as pd
import time
from threading import Thread
from utils import stop_timer
from scrape_utils import ScrapeSession


class Worker(Thread):
    def __init__(self, scrape_session, request_queue):
        Thread.__init__(self)
        self.scrape_session = scrape_session
        self.queue = request_queue
        self.results = None
        self.type = None

    def run(self):
        while True:
            content = self.queue.get()
            if content == '':
                break
            t, func, arguments = content
            self.results = (t, func(**arguments))
            self.queue.task_done()


class Scrape:
    def __init__(self):
        self.scrape_session = ScrapeSession()


    def scrape(self, splits, no_workers):
        # Create queue and add splits
        q = queue.Queue()
        for split in splits:
            q.put(split)

        # Create workers and add to the queue
        workers = list()
        for _ in range(no_workers):
            worker = Worker(self.scrape_session, q)
            worker.start()
            workers.append(worker)

        # Workers keep working till they receive an empty string
        for _ in workers:
            q.put('')

        # Join workers to wait till they finished
        for worker in workers:
            worker.join()

        # Combine results from all workers
        statcast_dfs = list()
        results_dict = dict()
        for worker in workers:
            worker_type, results = worker.results
            if worker_type == 'statcast':
                statcast_dfs.append(results)
            else:
                results_dict[worker_type] = results
        results_dict['statcast'] = pd.concat(statcast_dfs, ignore_index=True).rename({'estimated_ba_using_speedangle': 'xBA'}, axis=1).sort_values(by=['game_date', 'game_pk', 'at_bat_number'], ignore_index=True)

        return results_dict


    def get_data(self, date=None, is_today=False):
        start_time = time.time() # Start timer

        split_num = 1
        jobs = list()
        for inning in range(1, 10):
            for home_away in ['Home', 'Road']:
                jobs.append(('statcast', self.scrape_session.read_statcast_csv, {'home_away': home_away, 'inning': inning, 'date': date}))
                split_num += 1

        jobs.append(('player_info', self.scrape_session.get_player_info, {'year': date.year, 'hitters': True, 'pitchers': True}))
        if is_today:
            jobs.append(('head_to_head', self.scrape_session.batter_vs_pitcher, dict()))
            jobs.append(('weather', self.scrape_session.get_weather, dict()))
            jobs.append(('todays_games', self.scrape_session.get_schedule, {'year': date.year, 'date': date, 'lineups': True, 'is_today': True}))
            jobs.append(('injured_player_ids', self.scrape_session.injured_player_ids, {'year': date.year}))

        scraped_data = self.scrape(jobs, len(jobs)) # one worker for each job

        stop_timer('get_data()', start_time) # Stop timer
        return scraped_data