import asyncio
import json
import typing
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from loguru import logger

from dbworker import DBWorker1, DBWorker2


@dataclass
class Answer:
    id: int
    text: str
    step_id: int
    is_right: bool
    question_id: int

@dataclass
class UserAnswer:
    # {"start_time": 1591201353, "correct_answer": "\u0413", "actual_answer": "\u0412", "end_time": 1591201394}
    start_time: datetime
    correct_answer: str
    actual_answer: str
    end_time: datetime

@dataclass
class Question:
    id: int
    text: str
    answers: List[Answer]

@dataclass
class UserTestResult:
    right: int
    wrong: int
    step: int
    is_ended: bool


def first_or_default_answer(list_: List[Answer], answer: UserAnswer, step_id: int) -> Optional[Answer]:
    return next((e for e in list_ if e.text == answer.actual_answer and e.step_id == step_id), None)

async def load_answers_from_db(db: DBWorker2) -> typing.List[Answer]:
    res = []
    for i in range(0, 25, 1):
        ans = await db.get_answers_by_step(i)
        for a in ans:
            res.append(Answer(**a))
    
    return res

async def upload_answers(db2: DBWorker2, db1: DBWorker1, user_answers: List[UserAnswer], answers_db: List[Answer], user_id: int, current_test_id: int = 1) -> UserTestResult:
    result = UserTestResult(0, 0, 1, False)
    logger.info(f"Start calculating result for user {user_id}")
    for answer in user_answers:
        logger.info(f"Calculating step {result.step} r {result.right} w {result.wrong}")
        if answer.actual_answer == answer.correct_answer:
            result.right += 1

            answer_db = next((i for i in answers_db if i.step_id == result.step and i.is_right == True), None)
            logger.info(f"Correct answer, answer db {answer_db}")
            if answer_db:
                await db2.write_answer(current_test_id, user_id, answer_db.id, answer_db.question_id)
        else:
            result.wrong += 1

            answer_db = next((i for i in answers_db if i.step_id == result.step and i.is_right == False), None)
            logger.info(f"Incorrect answer, answer db {answer_db}")
            if answer_db:
                await db2.write_answer(current_test_id, user_id, answer_db.id, answer_db.question_id)
        

        result.step += 1
    

    await db2.update_current_test_step(user_id, current_test_id, len(user_answers) + 1)

    result.is_ended = len(user_answers) == 25

    return result

async def main(db1: DBWorker1, db2: DBWorker2):
    PAGE = 1
    PER_PAGE = 100

    ANSWERS_SET = await load_answers_from_db(db2)

    TEST_ID = 1
    STEP_START_FROM = 1

    while (True):
        logger.info(f"Fetching page {PAGE}, per page {PER_PAGE}...")
        results = await db1.get_results(PAGE, PER_PAGE)
        logger.info(f"Fetched page {PAGE}, count {len(results)}")
        try:
            for result in results:
                payload = result['result']
                user_id = result['user_id']
                user_attempts = json.loads(payload)
                logger.info(f"Start uploading attempts for user {user_id}")

                for n, attempt in enumerate(user_attempts.values()):
                    logger.info(f"Uploading attempt {n} user {user_id}")
                    if not attempt['answers']:
                        logger.info(f"Answers for this attempt was empty")
                        break

                    attempt_start_time = datetime.fromtimestamp(attempt['start_time'])
                    attempt_end_time = None
                    if attempt['end_time']:
                        attempt_end_time = datetime.fromtimestamp(attempt['end_time'])

                    
                    logger.info(f"Attempt {n} user {user_id} started {attempt_start_time.isoformat()}, ended {attempt_end_time.isoformat() if attempt_end_time is not None else 'user haven;t done this attempt'}")

                    current_test_id = await db2.set_current_test(user_id, TEST_ID, STEP_START_FROM, attempt_start_time)
                    logger.info(f"Test id {current_test_id}")

                    user_answers = [UserAnswer(**i) for i in attempt['answers']]
                    
                    user_result = await upload_answers(db2, db1, user_answers, ANSWERS_SET, user_id, current_test_id)
                    logger.info(f"{user_result}")

                    if user_result.is_ended:
                        interval = attempt_end_time - attempt_start_time
                        
                        await db2.write_result(current_test_id, user_id, interval, user_result.right, user_result.wrong, current_test_id)
        except Exception:
            logger.opt(exception=True).debug("Exception logged with debug level:")

        PAGE += 1


db1: DBWorker1 = DBWorker1()
loop = asyncio.get_event_loop()
loop.run_until_complete(db1.connect())
db2: DBWorker2 = DBWorker2()
loop.run_until_complete(db2.connect())

    
loop.run_until_complete(main(db1, db2))
