postgres_ip = "134.122.86.135"
postgres_port = "5432"
postgres_database = "postgres"
postgres_user = "postgres"
postgres_password = "clap55?kicks"

old_postgres_ip = "167.99.8.165"
old_postgres_port = "5432"
old_postgres_database = "test"
old_postgres_user = "admin"
old_postgres_password = "Ada2}karate123"


from loguru import logger
import asyncpg

class DBWorker1:

    async def connect(self, migrate: bool = False) -> None:
        self.conn = await asyncpg.connect(
            user=old_postgres_user,
            password=old_postgres_password,
            database=old_postgres_database,
            host=old_postgres_ip,
        )
    async def get_results(self, page = 1, per_page = 100):
        result = await self.conn.fetch(get_test_results_from_old_db, per_page, (page - 1) * per_page)

        if len(result) > 0:
            return result

        return []

class DBWorker2:
    async def connect(self, migrate: bool = False) -> None:
        self.conn = await asyncpg.connect(
            user=postgres_user,
            password=postgres_password,
            database=postgres_database,
            host=postgres_ip,
        )


    async def get_answers_by_step(self, step):
        answers = await self.conn.fetch(get_answers_by_step, step)
        return answers
    
    async def set_current_test(self, user_id, test_id, step, start_time):
        result = await self.conn.fetchrow(set_current_test, user_id, test_id, step, start_time)
        logger.info(result)
        if len(result)>0:
            return result['id']
        return False
    
    async def write_answer(self, curr_id, user_id, ans_id, q_id):
        await self.conn.execute(write_answer, curr_id, user_id, ans_id, q_id)
    
    async def update_current_test_step(self, user_id,curr_id, step):
        await self.conn.execute(update_current_test_step, curr_id, user_id, step)
    
    async def write_result(self, curr_id, user_id, time, r, w, test_id):
        await self.conn.execute(write_result, curr_id, user_id, time, r, w, test_id)



write_result = "insert into results(\"id\", \"user_id\", \"time\", \"right\", \"wrong\", \"test_id\") values($1,$2,$3,$4,$5,$6)"
update_current_test_step = "update current_test set step=$3 WHERE id=$1 and user_id = $2"
write_answer = "insert into user_answers(id, user_id, answer_id, question_id) values($1, $2, $3, $4)"
set_current_test = "insert into current_test(\"user_id\", \"test_id\", \"step\", \"start_time\") values($1, $2, $3, $4) returning id"
get_answers_by_step = "select a.id, a.is_right, a.question_id, a.text, tq.step as step_id from test_questions tq join answers a on tq.question_id = a.question_id where tq.step=$1"
get_test_results_from_old_db = "select * from results LIMIT $1 offset $2"
get_results_from_old_db = "select * from results"
set_user_to_new_db = "insert into users(id, name) values($1,$2) returning id"
write_test_question = "insert into test_questions(test_id, question_id, step) values(1,$1,$2) returning id"