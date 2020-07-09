from datetime import datetime
from logging import log
from loguru import logger
from poll.dbworker import DBWorker1, DBWorker2
import test
import asyncio
import json




db1: DBWorker1 = DBWorker1()
loop = asyncio.get_event_loop()
loop.run_until_complete(db1.connect())
db2: DBWorker2 = DBWorker2()
loop.run_until_complete(db2.connect())

async def main():
    users = await db1.get_users()
    logger.info(len(users))
    for user in users:
        current_poll = 0
        gender_from_db = await db2.get_poll_answers(1)
        if user['gender'] in gender_from_db.keys():
            gender_from_db[user['gender']]
            await db2.write_poll_answer(user['id'], gender_from_db['id'], gender_from_db['question_id'])
            current_poll += 1
            age_from_db = await db2.get_poll_answers(2)
            if user['age'] in age_from_db.keys():
                age_from_db[user['age']]
                await db2.write_poll_answer(user['id'], age_from_db['id'], age_from_db['question_id'])
                current_poll += 1
                education_from_db = await db2.get_poll_answers(3)
                if user['education'] in education_from_db.keys():
                    education_from_db[user['education']]
                    await db2.write_poll_answer(user['id'], education_from_db['id'], education_from_db['question_id'])
                    current_poll += 1
                    brain_type_from_db = await db2.get_poll_answers(4)
                    if user['brain_type'] in brain_type_from_db.keys():
                        brain_type_from_db[user['brain_type']]
                        await db2.write_poll_answer(user['id'], brain_type_from_db['id'], brain_type_from_db['question_id'])
                        current_poll += 1
                        income_from_db = await db2.get_poll_answers(5)
                        if user['income'] in brain_type_from_db.keys():
                            income_from_db[user['income']]
                            await db2.write_poll_answer(user['id'], income_from_db['id'], income_from_db['question_id'])
                            current_poll += 1
        await db2.set_current_poll(current_poll+1)
                        


