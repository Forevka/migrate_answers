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
    async def get_users(self):
        result = await self.conn.fetch(get_users_from_old_db)
        logger.info(result)
        if len(result)>0:
            return result
        return False

class DBWorker2:
    async def connect(self, migrate: bool = False) -> None:
        self.conn = await asyncpg.connect(
            user=postgres_user,
            password=postgres_password,
            database=postgres_database,
            host=postgres_ip,
        )
    
    async def get_poll_answers(self, quesiton_id):
        result = await self.conn.fetch(get_users_from_old_db, quesiton_id)
        l = {}
        for i in result:
            l[i['text']] = {'id':i['id'], 'question_id': i['question_id']}
        return l

    async def write_poll_answer(self, user_id, answer_id, question_id):
        await self.conn.execute(get_users_from_old_db, question_id, user_id, answer_id)
    
    async def set_current_poll(self, user_id, step):
        await self.conn.execute(set_current_poll, user_id, step)

set_current_poll = "insert into current_poll(\"user_id\", \"step\") values($1, 1) returning *"
write_poll_answer = "insert into user_poll_answers(question_id, user_id, answer_id) values($1, $2, $3)"
get_poll_answers = "select * from poll_andwers where queston_id = $1"
get_users_from_old_db = "select * from results"