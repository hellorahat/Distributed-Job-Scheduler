from app.task_registry import register_task


@register_task('task.echo')
def echo_task(payload: dict) -> None:
    print('TASK:', payload)
