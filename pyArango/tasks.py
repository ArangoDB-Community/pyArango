"""All Task related methods."""


class Tasks:
    """Tasks for database."""

    URL = '/_api/tasks'

    def __init__(self, database):
        """Initialise the database."""
        self.database = database

    def __call__(self):
        """All the active tasks in the db."""
        # response = self.database.action.get(self.URL)
        # response.raise_for_status()
        # return response.json()
        return self.fetch()

    def drop(self):
        """delete all tasks"""
        for task in self.fetch():
            self.delete(task["id"])

    def fetch(self, task_id=None):
        """Fetch the task for given task_id. If task_id is None return all tasks """
        if task_id is not None:
            url = '{tasks_url}/{task_id}'.format(
                tasks_url=self.URL, task_id=task_id
            )
        else:
            url = self.URL

        response = self.database.action.get(url)
        response.raise_for_status()
        return response.json()

    def create(
            self, name, command, params=None,
            period=None, offset=None, task_id=None
    ):
        """Create a task with given command and its parameters."""
        task = {'name': name, 'command': command, 'params': params}
        if period is not None:
            task['period'] = period
            if offset is not None:
                task['offset'] = offset

        if task_id is not None:
            task['id'] = task_id
            url = '{tasks_url}/{task_id}'.format(
                tasks_url=self.URL, task_id=task_id
            )
        else:
            url = self.URL

        response = self.database.action.post(url, json=task)
        response.raise_for_status()
        return response.json()

    def delete(self, task_id):
        """Delete the task for given task_id."""
        url = '{tasks_url}/{task_id}'.format(
            tasks_url=self.URL, task_id=task_id
        )
        response = self.database.action.delete(url)
        response.raise_for_status()
        return response.json()
