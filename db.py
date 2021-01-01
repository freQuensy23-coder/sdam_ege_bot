import pymysql
from pymysql.cursors import DictCursor
import config
import logging

log = logging.getLogger("db")


class DB:
    def __init__(self):
        self.connection = pymysql.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            db=config.db,
            charset=config.charset,
            cursorclass=DictCursor
        )
        self.cursor = self.connection.cursor()
        timeout = 2147482
        self.cursor.execute(query=f"""SET SESSION wait_timeout := {timeout};""")
        self.connection.commit()
        log.debug("db inited")

    def get_all_tasks(self):
        log.debug("Get all task")
        query = """SELECT * FROM Tasks_math"""
        self.cursor.execute(query)
        task_ids = (self.cursor.fetchall())
        return task_ids

    def add_task(self, task_id, text, answer, solution, images_tasks, images_solution, task_num, ok):
        query = f"""
        INSERT INTO Tasks_math VALUES (task_id, text, answer, solution, images_taks, images_solution, task_num, Ok)    
        {task_id}, '{text}', '{answer}', '{solution}', '{repr(images_tasks)}', '{repr(images_solution)}', {task_num}, {ok}         
        """
        log.info(f"Add task with task_id={task_id}, text='{text}', answer='{answer}', solution='{solution}',"
                 f" images_task='{repr(images_tasks)}', images_solution='{repr(images_solution)}', num={task_num}, ok={ok}")
        self.cursor.execute(query)
        self.connection.commit()

    def update_task(self, task_id, text, answer, solution, images_tasks, images_solution, task_num, ok):
        query = f"""
        UPDATE Tasks_math 
        SET text = '{text}',
            answer = '{answer}',
            solution = '{solution}',
            images_task = "{repr(images_tasks)}",
            images_solution = "{repr(images_solution)}",
            task_number = {task_num},
            ok = {int(ok)}
        WHERE task_id = {task_id}
        """
        log.info(f"Task with task_id={task_id} updated. text = '{text}', answer = '{answer}',solution = '{solution}'," +
                 f'images_task = "{repr(images_tasks)}'
                 + f'images_solution = "{repr(images_solution)}",task_number = {task_num},ok = {int(ok)}')
        self.cursor.execute(query)
        self.connection.commit()

    def get_task(self, not_valid_task: list, num: int):
        """Get random task from mysql.
        Not valid tasks - list of tasks_ids that could not be returned.
        Num - number of task in ege (1 - 8)."""
        not_is_str = ""
        for task in not_valid_task:
            not_is_str += f" AND task_number != {task} "
        q = f"""    
            SELECT *
            FROM Tasks_math
            WHERE ((task_number = {num})  {not_is_str}) 
            ORDER BY rand()
            LIMIT 10
            """
        cur = self.connection.cursor()
        cur.execute(q)
        task_data = cur.fetchone()
        log.debug(f"Get task with task+data = {task_data}")
        return task_data

    def get_task_data(self, task_id: int) -> dict:
        """Get all data from 1 task.
        Result - dict with fields
            user_id
            score
            reg_time
            name
            solved_problems
            wrong_solved
            current_problem_id
            status
        """
        q = f"""
            SELECT * FROM Tasks_math 
            WHERE task_id = {task_id}
        """
        cur = self.connection.cursor()
        cur.execute(q)
        task_data  = cur.fetchone
        log.debug(f"Get task data. Id = {task_id},  task+data = {task_data}")
        return task_data
