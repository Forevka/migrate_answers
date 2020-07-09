from datetime import datetime
from logging import log
from loguru import logger
from dbworker import DBWorker1, DBWorker2
import test
import asyncio
import json

db1: DBWorker1 = DBWorker1()
loop = asyncio.get_event_loop()
loop.run_until_complete(db1.connect())
db2: DBWorker2 = DBWorker2()
loop.run_until_complete(db2.connect())

class Question:
    def __init__(self, q_id=None, text=None, ans=None):
        self.id = q_id
        self.text = text
        self.answers = ans

logger.info("sadsadasdas") 
async def main():
    results = await db1.get_results()
    logger.info(len(results))
    for result in results:
        k = result['result']
        user_id = result['user_id']
        logger.info(k)
        logger.info(type(k))
        json_from_db = json.loads(k)
        for js in json_from_db.values():
            if len(js['answers'])>0:
                start_time = datetime.fromtimestamp(js['start_time'])
                if js['end_time']:
                    end_time = datetime.fromtimestamp(js['end_time'])
                current_test_id = await db2.set_current_test(user_id, 1, 1, start_time)
                right = 0
                wrong = 0
                step = 1
                for answer in js['answers']:
                    if answer['correct_answer'] == answer['actual_answer']:
                        is_right = True
                    else:
                        is_right = False
                    answers_from_db = await db2.get_answers_by_step(step)
                    for answer_from_db in answers_from_db:
                        if answer_from_db['is_right'] == is_right:
                            await db2.write_answer(current_test_id, user_id, answer_from_db['id'], answer_from_db['question_id'])
                            if is_right == True:
                                right+=1
                            else:
                                wrong+=1
                            break
                    step+=1
                await db2.update_current_test_step(user_id, current_test_id, len(js['answers'])+1)
                if len(js['answers']) == 25:
                    interval = end_time-start_time
                    logger.info(interval)
                    logger.info(type(interval))
                    await db2.write_result(current_test_id, user_id, interval, right, wrong, 1)
        #await db2.set_users(i)
    
loop.run_until_complete(main())

